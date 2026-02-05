import asyncio
import logging
import os
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
            logger.error(f"Unexpected error: {e}")
            await asyncio.sleep(2 ** attempt)
    raise Exception("Max retries exceeded")

# ================= HELPER: preserve original cover =================
async def _send_preserve_cover(client, message, chat_id, kwargs):
    thumb_path = None
    try:
        if getattr(message, "video", None) is not None:
            await handle_flood(client.copy_message, **kwargs)
            return True

        doc = getattr(message, "document", None)
        if doc and getattr(doc, "mime_type", "").startswith("video"):
            thumb_media = None
            if getattr(doc, "thumb", None):
                thumb_media = doc.thumb
            elif getattr(message, "thumbnail", None):
                thumb_media = message.thumbnail

            if thumb_media is not None:
                try:
                    thumb_path = await client.download_media(thumb_media)
                except Exception:
                    logger.exception("Failed to download original thumbnail; will try without thumb")
                    thumb_path = None

            send_kwargs = {
                "chat_id": chat_id,
                "video": doc.file_id,
                "caption": kwargs.get("caption"),
                "caption_entities": kwargs.get("caption_entities"),
                "supports_streaming": True,
            }

            duration = getattr(doc, "duration", None)
            width = getattr(doc, "width", None)
            height = getattr(doc, "height", None)
            if duration:
                send_kwargs["duration"] = duration
            if width:
                send_kwargs["width"] = width
            if height:
                send_kwargs["height"] = height
            if thumb_path:
                send_kwargs["thumb"] = thumb_path

            await handle_flood(client.send_video, **send_kwargs)
            return True

        await handle_flood(client.copy_message, **kwargs)
        return True

    except Exception as e:
        logger.error(f"Forward failed {getattr(message,'id',None)} -> {chat_id}: {e}")
        return False
    finally:
        if thumb_path:
            try:
                if os.path.exists(thumb_path):
                    os.remove(thumb_path)
            except Exception:
                logger.debug("Failed to remove temp thumb file", exc_info=True)

# ================= SINGLE FORWARD =================
async def forward_single_message(client, message, chat_id):
    kwargs = {
        "chat_id": chat_id,
        "from_chat_id": message.chat.id,
        "message_id": message.id,
    }

    if getattr(message, "caption", None):
        kwargs["caption"] = message.caption
        if getattr(message, "caption_entities", None):
            kwargs["caption_entities"] = message.caption_entities

    return await _send_preserve_cover(client, message, chat_id, kwargs)

# ================= BUFFER FORWARD =================
async def forward_buffered_messages(client, messages, chat_id):
    success = 0
    for msg in sorted(messages, key=lambda m: m.id):
        if await forward_single_message(client, msg, chat_id):
            success += 1
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

# ================= START/STOP PROCESSORS (call these from your bot) =================
async def start_processor(client):
    tasks = []
    for i in range(QUEUE_WORKERS):
        tasks.append(asyncio.create_task(process_queue(client)))
    logger.info(f"{QUEUE_WORKERS} queue workers started")
    return tasks

async def start_forwarder(client):
    """
    Call this after client.start()
    Example:
      await start_forwarder(app)
    """
    # prevent double-start
    if getattr(client, "_queue_tasks", None):
        return
    client._queue_tasks = await start_processor(client)

async def stop_forwarder(client, timeout: float = 5.0):
    """
    Call this before/after client.stop() to cleanup.
    Example:
      await stop_forwarder(app)
    """
    for t in getattr(client, "_queue_tasks", []):
        t.cancel()
    try:
        await asyncio.wait_for(message_queue.join(), timeout=timeout)
    except Exception:
        logger.info("Shutdown: queue join timeout or interrupted")
    client._queue_tasks = []

# ================= BUFFER HANDLER =================
async def process_buffered_messages(source_chat_id):
    try:
        await asyncio.sleep(BUFFER_DELAY)
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
        logger.debug(f"Buffer task for {source_chat_id} cancelled")
        raise
    except Exception:
        logger.exception("Unexpected error in buffer processor")
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
            old.cancel()

        buffer_tasks[cid] = asyncio.create_task(process_buffered_messages(cid))
    except Exception:
        logger.exception("Handler error")
