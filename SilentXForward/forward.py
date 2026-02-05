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
# If True, when copy_message or send_video-with-file_id fails to preserve thumbnail,
# bot will download full video and re-upload it with the thumbnail (heavy).
REUPLOAD_ON_MISSING_THUMB = True
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

# ================= HELPER: preserve original cover (full) =================
async def _send_preserve_cover(client, message, chat_id, kwargs):
    """
    Full strategy:
    1) Try copy_message (fast; usually preserves original thumb).
    2) If copy_message didn't preserve thumb and source has a thumb:
       a) Try send_video with source file_id + downloaded thumb.
       b) If that fails and REUPLOAD_ON_MISSING_THUMB=True: download full video and re-upload with thumb.
    3) If no thumb available on source, return whether copy_message succeeded.
    """
    thumb_path = None
    forwarded = None
    try:
        # Attempt 1: copy_message (fast server-side copy)
        try:
            forwarded = await handle_flood(client.copy_message, **kwargs)
        except Exception:
            logger.exception("copy_message failed; will attempt alternative send methods")
            forwarded = None

        # If copy_message succeeded, check if forwarded has thumbnail (video/document)
        if forwarded is not None:
            f_has_thumb = False
            if getattr(forwarded, "video", None) and getattr(forwarded.video, "thumb", None):
                f_has_thumb = True
            elif getattr(forwarded, "document", None) and getattr(forwarded.document, "thumb", None):
                f_has_thumb = True

            if f_has_thumb:
                logger.info("copy_message preserved thumbnail")
                return True

            logger.info("copy_message did not preserve thumbnail; attempting preserve steps")
        else:
            logger.info("copy_message returned no forwarded message; attempting preserve steps")

        # Find source thumbnail if available
        thumb_media = None
        if getattr(message, "video", None) and getattr(message.video, "thumb", None):
            thumb_media = message.video.thumb
        elif getattr(message, "document", None) and getattr(message.document, "thumb", None):
            thumb_media = message.document.thumb
        elif getattr(message, "thumbnail", None):
            thumb_media = message.thumbnail

        if thumb_media is None:
            logger.info("No thumbnail found on source message; nothing more to do")
            return forwarded is not None

        # Download thumbnail
        try:
            thumb_path = await client.download_media(thumb_media)
            logger.debug(f"Downloaded source thumbnail to {thumb_path}")
        except Exception:
            logger.exception("Failed to download source thumbnail; aborting preserve attempt")
            thumb_path = None
            return forwarded is not None

        # Try lightweight send: send_video using source file_id + thumb
        src_vid = getattr(message, "video", None)
        src_doc = getattr(message, "document", None)

        send_kwargs_base = {
            "chat_id": chat_id,
            "caption": kwargs.get("caption"),
            "caption_entities": kwargs.get("caption_entities"),
            "supports_streaming": True,
        }

        # If original was video object
        if src_vid is not None:
            send_kwargs = send_kwargs_base.copy()
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
                logger.exception("send_video with file_id+thumb failed")

        # If original was document with video mime
        if src_doc is not None and getattr(src_doc, "mime_type", "").startswith("video"):
            send_kwargs = send_kwargs_base.copy()
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
                logger.exception("send_video(document file_id)+thumb failed")

        # Heavy fallback: download full video and re-upload (only if enabled)
        if REUPLOAD_ON_MISSING_THUMB:
            logger.info("Attempting heavy re-upload fallback (download full video)")
            try:
                # Download full media (can be large)
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
                logger.exception("Heavy re-upload fallback failed")

        # Couldn't preserve thumbnail; return whether copy_message at least succeeded
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
    if getattr(client, "_queue_tasks", None):
        return
    client._queue_tasks = await start_processor(client)

async def stop_forwarder(client, timeout: float = 10.0):
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
        # Debug logging: helpful to see what the source message holds
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
