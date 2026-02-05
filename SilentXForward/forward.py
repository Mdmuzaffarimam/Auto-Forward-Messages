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

# If True -> heavy fallback: download full video and re-upload with thumb when needed.
# This guarantees identical cover but uses bandwidth.
REUPLOAD_ON_MISSING_THUMB = False
# ==========================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

message_queue = asyncio.Queue()
message_buffer = defaultdict(list)  # source_chat_id -> [messages]
buffer_tasks = {}  # source_chat_id -> asyncio.Task

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
    """
    Strategy:
    - If message.video exists -> use copy_message (preserves original thumb usually).
    - If message.document with video mime -> try send_video with file_id + thumb (downloaded).
    - If above fail and REUPLOAD_ON_MISSING_THUMB -> download full video and re-upload with thumb.
    - Fallback: copy_message.
    """
    thumb_path = None
    forwarded = None
    try:
        # Try fast copy first
        try:
            forwarded = await handle_flood(client.copy_message, **kwargs)
        except Exception:
            logger.debug("copy_message failed or raised; will try alternative send methods")

        # If copy_message succeeded and preserved a thumb, we're done
        if forwarded is not None:
            f_has_thumb = False
            if getattr(forwarded, "video", None) and getattr(forwarded.video, "thumb", None):
                f_has_thumb = True
            elif getattr(forwarded, "document", None) and getattr(forwarded.document, "thumb", None):
                f_has_thumb = True

            if f_has_thumb:
                logger.info("copy_message preserved thumbnail")
                return True
            logger.info("copy_message did not preserve thumbnail (or no forwarded object) — attempting preserve steps")

        # Determine source thumbnail if available
        thumb_media = None
        if getattr(message, "video", None) and getattr(message.video, "thumb", None):
            thumb_media = message.video.thumb
        elif getattr(message, "document", None) and getattr(message.document, "thumb", None):
            thumb_media = message.document.thumb
        elif getattr(message, "thumbnail", None):
            thumb_media = message.thumbnail

        if thumb_media is None:
            logger.info("No thumbnail found on source; returning whether copy_message succeeded")
            return forwarded is not None

        # Download thumbnail to temp file
        try:
            thumb_path = await client.download_media(thumb_media)
            logger.debug(f"Downloaded thumbnail to {thumb_path}")
        except Exception:
            logger.exception("Failed to download thumbnail; aborting preserve-attempt")
            thumb_path = None
            return forwarded is not None

        # Attempt lightweight send using file_id + thumb
        src_vid = getattr(message, "video", None)
        src_doc = getattr(message, "document", None)

        send_base = {
            "chat_id": chat_id,
            "caption": kwargs.get("caption"),
            "caption_entities": kwargs.get("caption_entities"),
            "supports_streaming": True,
        }

        # If original was a video message
        if src_vid is not None:
            send_kwargs = send_base.copy()
            send_kwargs.update({
                "video": src_vid.file_id,
                "duration": getattr(src_vid, "duration", None),
                "width": getattr(src_vid, "width", None),
                "height": getattr(src_vid, "height", None),
                "thumb": thumb_path,
            })
            try:
                await handle_flood(client.send_video, **{k: v for k, v in send_kwargs.items() if v is not None})
                logger.info("send_video using source file_id + thumb succeeded")
                return True
            except Exception:
                logger.exception("send_video(file_id+thumb) failed")

        # If original was a document with video mime
        if src_doc is not None and getattr(src_doc, "mime_type", "").startswith("video"):
            send_kwargs = send_base.copy()
            send_kwargs.update({
                "video": src_doc.file_id,
                "duration": getattr(src_doc, "duration", None),
                "width": getattr(src_doc, "width", None),
                "height": getattr(src_doc, "height", None),
                "thumb": thumb_path,
            })
            try:
                await handle_flood(client.send_video, **{k: v for k, v in send_kwargs.items() if v is not None})
                logger.info("send_video using document file_id + thumb succeeded")
                return True
            except Exception:
                logger.exception("send_video(document file_id+thumb) failed")

        # Heavy fallback: download full video and re-upload with thumb (optional)
        if REUPLOAD_ON_MISSING_THUMB:
            logger.info("Attempting heavy re-upload fallback (download full video)")
            try:
                video_path = await client.download_media(message)
                try:
                    send_kwargs = {
                        "chat_id": chat_id,
                        "video": video_path,
                        "thumb": thumb_path,
                        "caption": kwargs.get("caption"),
                        "caption_entities": kwargs.get("caption_entities"),
                        "supports_streaming": True,
                    }
                    await handle_flood(client.send_video, **{k: v for k, v in send_kwargs.items() if v is not None})
                    logger.info("Heavy re-upload with thumb succeeded")
                    return True
                finally:
                    try:
                        if os.path.exists(video_path):
                            os.remove(video_path)
                    except Exception:
                        logger.debug("Failed to remove temp video file", exc_info=True)
            except Exception:
                logger.exception("Heavy re-upload failed")

        # If all else fails, return whether copy_message at least succeeded
        return forwarded is not None

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

# ================= START PROCESSORS (backwards-compatible) =================
async def start_processor(client):
    """
    Starts QUEUE_WORKERS queue worker tasks and returns a dict of tasks
    to match existing bot.py usage (so bot can call .values() and cancel them).
    """
    tasks = {}
    for i in range(QUEUE_WORKERS):
        t = asyncio.create_task(process_queue(client))
        tasks[f"worker_{i}"] = t
    logger.info(f"{QUEUE_WORKERS} queue workers started")
    return tasks

# ================= STOP helper (optional) =================
async def stop_processor(tasks: dict, timeout: float = 5.0):
    """
    Cancel worker tasks and optionally wait for queue to drain.
    """
    for t in tasks.values():
        t.cancel()
    try:
        await asyncio.wait_for(message_queue.join(), timeout=timeout)
    except Exception:
        logger.info("Shutdown: queue join timeout or interrupted")

# ================= BUFFER HANDLER =================
async def process_buffered_messages(source_chat_id):
    try:
        await asyncio.sleep(BUFFER_DELAY)

        # Atomically pop buffered messages to avoid races with new arrivals
        messages = message_buffer.pop(source_chat_id, None)
        if not messages:
            return

        try:
            mappings = await database.get_all_targets_for_source(source_chat_id)
            for mapping in mappings:
                targets = mapping.get("target_ids", [])
                if targets:
                    await message_queue.put((messages.copy(), targets, "buffered"))
                    logger.info(f"Queued {len(messages)} msgs from {source_chat_id} -> {len(targets)} targets")
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
        # debug info
        logger.debug(f"Received message id={getattr(message,'id',None)} from chat={getattr(message.chat,'id',None)}")
        if message.video and getattr(message.video, "thumb", None):
            logger.debug(f"Source has video.thumb: {message.video.thumb.file_id}")
        if message.document and getattr(message.document, "thumb", None):
            logger.debug(f"Source document has thumb: {message.document.thumb.file_id}")

        cid = message.chat.id
        message_buffer[cid].append(message)

        old = buffer_tasks.get(cid)
        if old and not old.done():
            old.cancel()

        buffer_tasks[cid] = asyncio.create_task(process_buffered_messages(cid))
    except Exception:
        logger.exception("Handler error")
