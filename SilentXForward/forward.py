import asyncio
import logging
from collections import defaultdict

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError

from SilentXForward import database

# ================= CONFIG =================
BUFFER_DELAY = 2.0
QUEUE_WORKERS = 3
TARGET_CONCURRENCY = 3
MSG_DELAY = 0.15
TARGET_DELAY = 0.2
MAX_RETRY = 3
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

message_queue = asyncio.Queue()

message_buffer = defaultdict(list)
buffer_tasks = {}

# ================= UTILS =================
def get_buffer_key(message):
    return message.media_group_id or message.id

# ================= FLOOD HANDLER =================
async def safe_call(func, *args, **kwargs):
    for attempt in range(MAX_RETRY):
        try:
            return await func(*args, **kwargs)
        except FloodWait as e:
            logger.warning(f"FloodWait {e.value}s")
            await asyncio.sleep(e.value + 1)
        except RPCError as e:
            logger.error(f"RPCError: {e}")
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            await asyncio.sleep(2 ** attempt)
    return None

# ================= SEND SINGLE MESSAGE =================
async def forward_single(client, message, target_id):
    try:
        # -------- TEXT --------
        if message.text:
            await safe_call(
                client.copy_message,
                chat_id=target_id,
                from_chat_id=message.chat.id,
                message_id=message.id
            )
            return True

        # -------- PHOTO --------
        if message.photo:
            await safe_call(
                client.send_photo,
                chat_id=target_id,
                photo=message.photo.file_id,
                caption=message.caption,
                caption_entities=message.caption_entities
            )
            return True

        # -------- VIDEO (WITH COVER) --------
        if message.video:
            thumb = None
            if message.video.thumbs:
                try:
                    thumb = await client.download_media(
                        message.video.thumbs[-1].file_id
                    )
                except Exception:
                    thumb = None

            await safe_call(
                client.send_video,
                chat_id=target_id,
                video=message.video.file_id,
                caption=message.caption,
                caption_entities=message.caption_entities,
                thumb=thumb,
                supports_streaming=True
            )
            return True

        # -------- DOCUMENT --------
        if message.document:
            thumb = None
            if message.document.thumbs:
                try:
                    thumb = await client.download_media(
                        message.document.thumbs[-1].file_id
                    )
                except Exception:
                    thumb = None

            await safe_call(
                client.send_document,
                chat_id=target_id,
                document=message.document.file_id,
                caption=message.caption,
                caption_entities=message.caption_entities,
                thumb=thumb
            )
            return True

        # -------- AUDIO / STICKER / ANIMATION --------
        await safe_call(
            client.copy_message,
            chat_id=target_id,
            from_chat_id=message.chat.id,
            message_id=message.id
        )
        return True

    except Exception as e:
        logger.error(f"Forward failed -> {target_id}: {e}")
        return False

# ================= BUFFER FORWARD =================
async def forward_buffer(client, messages, target_id):
    success = 0
    for msg in sorted(messages, key=lambda m: m.id):
        if await forward_single(client, msg, target_id):
            success += 1
        await asyncio.sleep(MSG_DELAY)

    logger.info(f"Buffered {success}/{len(messages)} -> {target_id}")
    return success > 0

# ================= QUEUE WORKER =================
async def queue_worker(client):
    sem = asyncio.Semaphore(TARGET_CONCURRENCY)

    async def handle_target(target_id, payload):
        async with sem:
            return await forward_buffer(client, payload, target_id)

    while True:
        payload, targets = await message_queue.get()
        failed = []

        tasks = [handle_target(t, payload) for t in targets]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for t, r in zip(targets, results):
            if r is not True:
                failed.append(t)

        if failed:
            logger.warning(f"Retrying {len(failed)} targets")
            await message_queue.put((payload, failed))

        message_queue.task_done()
        await asyncio.sleep(TARGET_DELAY)

# ================= START WORKERS =================
async def start_workers(client):
    for i in range(QUEUE_WORKERS):
        asyncio.create_task(queue_worker(client))
    logger.info(f"{QUEUE_WORKERS} queue workers started")

# ================= BUFFER PROCESS =================
async def process_buffer(key, source_chat_id):
    await asyncio.sleep(BUFFER_DELAY)

    messages = message_buffer.pop(key, [])
    buffer_tasks.pop(key, None)

    if not messages:
        return

    try:
        mappings = await database.get_all_targets_for_source(source_chat_id)
        for m in mappings:
            targets = m.get("target_ids", [])
            if targets:
                await message_queue.put((messages.copy(), targets))
                logger.info(
                    f"Queued {len(messages)} msgs -> {len(targets)} targets"
                )
    except Exception as e:
        logger.error(f"Buffer error: {e}")

# ================= MESSAGE HANDLER =================
@Client.on_message(
    filters.channel &
    (
        filters.video |
        filters.document |
        filters.photo |
        filters.audio |
        filters.animation |
        filters.sticker |
        filters.text
    )
)
async def on_new_message(client, message):
    try:
        key = get_buffer_key(message)
        message_buffer[key].append(message)

        if key not in buffer_tasks:
            buffer_tasks[key] = asyncio.create_task(
                process_buffer(key, message.chat.id)
            )

    except Exception:
        logger.exception("Message handler error")

# ================= APP START =================
async def main():
    app = Client("MRN_FORWARD_BOT")
    await app.start()
    await start_workers(app)
    logger.info("Bot started successfully")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
