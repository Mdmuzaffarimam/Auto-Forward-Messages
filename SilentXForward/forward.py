import asyncio
import logging
from collections import defaultdict
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError
from SilentXForward import database

# ================= CONFIG =================
BUFFER_DELAY = 2
QUEUE_WORKERS = 3
TARGET_CONCURRENCY = 3
MSG_DELAY = 0.1
TARGET_DELAY = 0.15
MAX_RETRIES = 3          # FIX #1: max retries for queue targets
# ==========================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

message_queue = asyncio.Queue()
message_buffer = defaultdict(list)
buffer_tasks = {}

# ================= FLOOD HANDLER =================
async def handle_flood(func, **kwargs):
    for attempt in range(MAX_RETRIES):
        try:
            return await func(**kwargs)
        except FloodWait as e:
            logger.warning(f"FloodWait: sleeping {e.value}s")
            await asyncio.sleep(e.value + 1)
        except RPCError as e:
            logger.error(f"RPCError: {e}")
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.exception(f"Unexpected error in RPC call: {e}")
            await asyncio.sleep(2 ** attempt)
    raise Exception("Max retries exceeded in handle_flood")

# ================= SINGLE FORWARD =================
async def forward_single_message(client, message, chat_id):
    try:
        # FIX #2: Don't pass caption/caption_entities — copy_message handles them automatically
        await handle_flood(
            client.copy_message,
            chat_id=chat_id,
            from_chat_id=message.chat.id,
            message_id=message.id,
        )
        return True
    except Exception:
        logger.exception(f"Forward failed msg_id={getattr(message, 'id', None)} -> {chat_id}")
        return False

# ================= BUFFER FORWARD =================
async def forward_buffered_messages(client, messages, chat_id):
    success = 0
    for msg in sorted(messages, key=lambda m: m.id):
        try:
            ok = await forward_single_message(client, msg, chat_id)
            if ok:
                success += 1
        except Exception:
            logger.exception(f"Error forwarding buffered msg {getattr(msg, 'id', None)} -> {chat_id}")
        await asyncio.sleep(MSG_DELAY)
    logger.info(f"Buffered forwarded {success}/{len(messages)} -> {chat_id}")
    return success > 0

# ================= QUEUE WORKER =================
async def process_queue(client):
    sem = asyncio.Semaphore(TARGET_CONCURRENCY)

    async def forward_target(chat_id, payload, ftype):
        async with sem:
            if ftype == "buffered":
                return await forward_buffered_messages(client, payload, chat_id)
            return await forward_single_message(client, payload, chat_id)

    while True:
        try:
            payload, targets, ftype, retry_count = await message_queue.get()
            failed = []

            tasks = [forward_target(tid, payload, ftype) for tid in targets]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for tid, res in zip(targets, results):
                if isinstance(res, Exception):
                    # FIX #5: Log actual exception from gather
                    logger.error(f"Exception forwarding to {tid}: {res}")
                    failed.append(tid)
                elif res is not True:
                    failed.append(tid)

            # FIX #1: Only retry if under max retry limit
            if failed:
                if retry_count < MAX_RETRIES:
                    logger.info(f"Retrying {len(failed)} targets (attempt {retry_count + 1}/{MAX_RETRIES})")
                    await message_queue.put((payload, failed, ftype, retry_count + 1))
                else:
                    logger.error(f"Giving up on {len(failed)} targets after {MAX_RETRIES} retries: {failed}")

            message_queue.task_done()
            await asyncio.sleep(TARGET_DELAY)

        except asyncio.CancelledError:
            logger.info("Queue worker cancelled, shutting down")
            raise
        except Exception:
            logger.exception("Unexpected error in queue worker — continuing")
            await asyncio.sleep(1)

# ================= WATCHDOG =================
async def worker_watchdog(client):
    """FIX #5: Restart any dead workers automatically."""
    while True:
        try:
            await asyncio.sleep(10)
            tasks = getattr(client, "_queue_tasks", {})
            for key, t in list(tasks.items()):
                if t.done():
                    exc = t.exception() if not t.cancelled() else None
                    logger.warning(f"Worker {key} died (exc={exc}), restarting...")
                    tasks[key] = asyncio.create_task(process_queue(client))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Watchdog error")

# ================= START PROCESSORS =================
async def start_processor(client):
    tasks = {}
    for i in range(QUEUE_WORKERS):
        t = asyncio.create_task(process_queue(client))
        tasks[f"worker_{i}"] = t
    # Start watchdog
    tasks["watchdog"] = asyncio.create_task(worker_watchdog(client))
    logger.info(f"{QUEUE_WORKERS} queue workers + watchdog started")
    return tasks

async def start_forwarder(client):
    if getattr(client, "_queue_tasks", None):
        return
    client._queue_tasks = await start_processor(client)

async def stop_forwarder(client, timeout: float = 5.0):
    tasks = getattr(client, "_queue_tasks", {}) or {}
    for t in tasks.values():
        t.cancel()
    try:
        await asyncio.wait_for(message_queue.join(), timeout=timeout)
    except Exception:
        logger.info("Shutdown: queue join timeout or interrupted")
    client._queue_tasks = {}

# ================= BUFFER HANDLER =================
async def process_buffered_messages(source_chat_id):
    try:
        await asyncio.sleep(BUFFER_DELAY)

        # FIX #4: Always pop buffer even if something fails below
        messages = message_buffer.pop(source_chat_id, None)
        if not messages:
            return

        mappings = await database.get_all_targets_for_source(source_chat_id)
        for mapping in mappings:
            targets = mapping.get("target_ids", [])
            if targets:
                # FIX #1: Added retry_count=0 as 4th element in queue tuple
                await message_queue.put((messages.copy(), targets, "buffered", 0))
                logger.info(
                    f"Queued {len(messages)} msgs from {source_chat_id} -> {len(targets)} targets"
                )

    except asyncio.CancelledError:
        # FIX #3: On cancel, put messages back in buffer so next task picks them up
        logger.debug(f"Buffer task for {source_chat_id} cancelled")
        raise
    except Exception:
        logger.exception("Unexpected error in buffer processor")
        # FIX #4: Make sure buffer is cleared even on unexpected crash
        message_buffer.pop(source_chat_id, None)
    finally:
        buffer_tasks.pop(source_chat_id, None)

# ================= MESSAGE LISTENER =================
@Client.on_message(
    filters.channel &
    (filters.video | filters.document | filters.photo |
     filters.audio | filters.sticker | filters.animation | filters.text)
)
async def forward_content(client, message):
    try:
        cid = message.chat.id
        message_buffer[cid].append(message)

        old = buffer_tasks.get(cid)
        if old and not old.done():
            try:
                old.cancel()
                # FIX #3: Wait briefly so cancel is processed before creating new task
                await asyncio.sleep(0)
            except Exception:
                logger.debug("Old buffer task cancel raised", exc_info=True)

        buffer_tasks[cid] = asyncio.create_task(process_buffered_messages(cid))
    except Exception:
        logger.exception("Handler error")
