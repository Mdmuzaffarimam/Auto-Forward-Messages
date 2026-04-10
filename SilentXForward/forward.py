import asyncio
import logging
import os
from collections import defaultdict
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message
from SilentXForward import database

# ================= CONFIG =================
BUFFER_DELAY = 0.5
QUEUE_WORKERS = 20
TARGET_CONCURRENCY = 50
MSG_DELAY = 0.05
TARGET_DELAY = 0.0
MAX_RETRIES = 3
BATCH_SIZE = 50
BATCH_REST = 0.2
THUMB_DIR = "thumbs"  # thumbnail cache folder
# ==========================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

os.makedirs(THUMB_DIR, exist_ok=True)

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

# ================= THUMBNAIL EXTRACTOR =================
async def get_thumbnail(client: Client, message: Message) -> str | None:
    """
    Message se thumbnail path nikalta hai.
    Agar video/document mein thumb hai toh download karta hai.
    """
    try:
        thumb = None

        if message.video and message.video.thumbs:
            thumb = message.video.thumbs[0]
        elif message.document and message.document.thumbs:
            thumb = message.document.thumbs[0]

        if thumb:
            thumb_path = os.path.join(THUMB_DIR, f"{message.id}_{thumb.file_id}.jpg")
            # Already downloaded hai toh reuse karo
            if not os.path.exists(thumb_path):
                await client.download_media(thumb.file_id, file_name=thumb_path)
            return thumb_path

    except Exception:
        logger.warning(f"Thumbnail extract failed for msg_id={message.id}")

    return None

# ================= SINGLE FORWARD =================
async def forward_single_message(client: Client, message: Message, chat_id: int):
    try:
        thumb_path = await get_thumbnail(client, message)

        extra = {}
        if thumb_path:
            extra["thumb"] = thumb_path

        await handle_flood(
            client.copy_message,
            chat_id=chat_id,
            from_chat_id=message.chat.id,
            message_id=message.id,
            **extra,
        )
        return True
    except Exception:
        logger.exception(f"Forward failed msg_id={getattr(message, 'id', None)} -> {chat_id}")
        return False

# ================= THUMBNAIL CLEANUP =================
async def cleanup_thumb(thumb_path: str | None):
    """Forward ke baad thumbnail delete karo disk space bachao."""
    try:
        if thumb_path and os.path.exists(thumb_path):
            os.remove(thumb_path)
    except Exception:
        pass

# ================= BUFFER FORWARD =================
async def forward_buffered_messages(client: Client, messages: list, chat_id: int):
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
async def process_queue(client: Client):
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

            # ---- BATCH PROCESSING ----
            for i in range(0, len(targets), BATCH_SIZE):
                batch = targets[i : i + BATCH_SIZE]
                tasks = [forward_target(tid, payload, ftype) for tid in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for tid, res in zip(batch, results):
                    if isinstance(res, Exception):
                        logger.error(f"Exception forwarding to {tid}: {res}")
                        failed.append(tid)
                    elif res is not True:
                        failed.append(tid)

                if i + BATCH_SIZE < len(targets):
                    await asyncio.sleep(BATCH_REST)
            # --------------------------

            # Thumbnail cleanup after all targets done
            if ftype == "buffered":
                for msg in payload:
                    thumb_path = os.path.join(
                        THUMB_DIR,
                        f"{msg.id}_*.jpg"
                    )
                    # glob se sab thumbnails clean karo
                    import glob
                    for f in glob.glob(thumb_path):
                        await cleanup_thumb(f)

            if failed:
                if retry_count < MAX_RETRIES:
                    logger.info(f"Retrying {len(failed)} targets (attempt {retry_count + 1}/{MAX_RETRIES})")
                    await message_queue.put((payload, failed, ftype, retry_count + 1))
                else:
                    logger.error(f"Giving up on {len(failed)} targets after {MAX_RETRIES} retries: {failed}")

            message_queue.task_done()

        except asyncio.CancelledError:
            logger.info("Queue worker cancelled, shutting down")
            raise
        except Exception:
            logger.exception("Unexpected error in queue worker — continuing")
            await asyncio.sleep(0.5)

# ================= WATCHDOG =================
async def worker_watchdog(client: Client):
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
async def start_processor(client: Client):
    tasks = {}
    for i in range(QUEUE_WORKERS):
        t = asyncio.create_task(process_queue(client))
        tasks[f"worker_{i}"] = t
    tasks["watchdog"] = asyncio.create_task(worker_watchdog(client))
    logger.info(f"{QUEUE_WORKERS} queue workers + watchdog started")
    return tasks

async def start_forwarder(client: Client):
    if getattr(client, "_queue_tasks", None):
        return
    client._queue_tasks = await start_processor(client)

async def stop_forwarder(client: Client, timeout: float = 5.0):
    tasks = getattr(client, "_queue_tasks", {}) or {}
    for t in tasks.values():
        t.cancel()
    try:
        await asyncio.wait_for(message_queue.join(), timeout=timeout)
    except Exception:
        logger.info("Shutdown: queue join timeout or interrupted")
    client._queue_tasks = {}

# ================= BUFFER HANDLER =================
async def process_buffered_messages(source_chat_id: int):
    try:
        await asyncio.sleep(BUFFER_DELAY)

        messages = message_buffer.pop(source_chat_id, None)
        if not messages:
            return

        mappings = await database.get_all_targets_for_source(source_chat_id)
        for mapping in mappings:
            targets = mapping.get("target_ids", [])
            if targets:
                await message_queue.put((messages.copy(), targets, "buffered", 0))
                logger.info(
                    f"Queued {len(messages)} msgs from {source_chat_id} -> {len(targets)} targets"
                )

    except asyncio.CancelledError:
        logger.debug(f"Buffer task for {source_chat_id} cancelled")
        raise
    except Exception:
        logger.exception("Unexpected error in buffer processor")
        message_buffer.pop(source_chat_id, None)
    finally:
        buffer_tasks.pop(source_chat_id, None)

# ================= MESSAGE LISTENER =================
@Client.on_message(
    filters.channel &
    (filters.video | filters.document | filters.photo |
     filters.audio | filters.sticker | filters.animation | filters.text)
)
async def forward_content(client: Client, message: Message):
    try:
        cid = message.chat.id
        message_buffer[cid].append(message)

        old = buffer_tasks.get(cid)
        if old and not old.done():
            try:
                old.cancel()
                await asyncio.sleep(0)
            except Exception:
                logger.debug("Old buffer task cancel raised", exc_info=True)

        buffer_tasks[cid] = asyncio.create_task(process_buffered_messages(cid))
    except Exception:
        logger.exception("Handler error")
