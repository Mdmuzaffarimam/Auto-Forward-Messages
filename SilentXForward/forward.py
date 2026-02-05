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
# ==========================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

message_queue = asyncio.Queue()
message_buffer = defaultdict(list)
buffer_tasks = {}

# ================= FLOOD HANDLER =================
async def handle_flood(func, **kwargs):
    retries = 3
    for attempt in range(retries):
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
    raise Exception("Max retries exceeded")

# ================= SINGLE FORWARD =================
async def forward_single_message(client, message, chat_id):
    try:
        kwargs = {
            "chat_id": chat_id,
            "from_chat_id": message.chat.id,
            "message_id": message.id,
        }

        # Defensive attribute access for caption / entities
        if getattr(message, "caption", None):
            kwargs["caption"] = message.caption
            if getattr(message, "caption_entities", None):
                kwargs["caption_entities"] = message.caption_entities

        await handle_flood(client.copy_message, **kwargs)
        return True
    except Exception:
        logger.exception(f"Forward failed {getattr(message, 'id', None)} -> {chat_id}")
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
            logger.exception(f"Error forwarding buffered msg {getattr(msg,'id',None)} -> {chat_id}")
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
        payload, targets, ftype = await message_queue.get()
        failed = []

        tasks = [forward_target(tid, payload, ftype) for tid in targets]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for tid, res in zip(targets, results):
            if res is not True:
                failed.append(tid)

        if failed:
            logger.info(f"Retrying {len(failed)} targets")
            await message_queue.put((payload, failed, ftype))

        message_queue.task_done()
        await asyncio.sleep(TARGET_DELAY)

# ================= START PROCESSORS =================
async def start_processor(client):
    """
    Starts queue workers and returns a dict of tasks.
    Returning a dict keeps compatibility with existing bot.py that expects .values().
    """
    tasks = {}
    for i in range(QUEUE_WORKERS):
        t = asyncio.create_task(process_queue(client))
        tasks[f"worker_{i}"] = t
    logger.info(f"{QUEUE_WORKERS} queue workers started")
    return tasks

# ================= BUFFER HANDLER =================
async def process_buffered_messages(source_chat_id):
    """
    Wait BUFFER_DELAY, then atomically take buffered messages and queue them.
    Handles cancellation (e.g., when a new message arrives and previous timer is cancelled).
    """
    try:
        await asyncio.sleep(BUFFER_DELAY)
        # Atomically take and remove the buffer for this source
        messages = message_buffer.pop(source_chat_id, None)
        if not messages:
            return

        try:
            mappings = await database.get_all_targets_for_source(source_chat_id)
            for mapping in mappings:
                targets = mapping.get("target_ids", [])
                if targets:
                    await message_queue.put((messages.copy(), targets, "buffered"))
                    logger.info(
                        f"Queued {len(messages)} msgs from {source_chat_id} -> {len(targets)} targets"
                    )
        except Exception:
            logger.exception("Buffer process error while queuing")
    except asyncio.CancelledError:
        # Normal: task was cancelled because a new message arrived and we rescheduled
        logger.debug(f"Buffer task for {source_chat_id} cancelled")
        raise
    except Exception:
        logger.exception("Unexpected error in buffer processor")
    finally:
        # Ensure we remove the task entry if present
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
        # Append to buffer
        message_buffer[cid].append(message)

        # If previous buffer task exists, cancel it and schedule a new one
        old = buffer_tasks.get(cid)
        if old and not old.done():
            try:
                old.cancel()
            except Exception:
                logger.debug("Old buffer task cancel raised", exc_info=True)

        # Schedule new buffer processing task
        buffer_tasks[cid] = asyncio.create_task(process_buffered_messages(cid))
    except Exception:
        logger.exception("Handler error")
