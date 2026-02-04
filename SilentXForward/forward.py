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
# ========================================

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
            await asyncio.sleep(e.value + 1)
        except RPCError:
            await asyncio.sleep(2 ** attempt)
        except Exception:
            await asyncio.sleep(2 ** attempt)
    raise Exception("Max retries exceeded")

# ================= THUMB DOWNLOAD =================
async def download_thumb(client, message):
    if message.video and message.video.thumbs:
        return await client.download_media(message.video.thumbs[0])
    if message.document and message.document.thumbs:
        return await client.download_media(message.document.thumbs[0])
    return None

# ================= SINGLE FORWARD (WITH COVER) =================
async def forward_single_message(client, message, chat_id):
    try:
        thumb = await download_thumb(client, message)

        if message.video:
            await handle_flood(
                client.send_video,
                chat_id=chat_id,
                video=message.video.file_id,
                caption=message.caption,
                caption_entities=message.caption_entities,
                duration=message.video.duration,
                width=message.video.width,
                height=message.video.height,
                thumb=thumb
            )

        elif message.document:
            await handle_flood(
                client.send_document,
                chat_id=chat_id,
                document=message.document.file_id,
                caption=message.caption,
                caption_entities=message.caption_entities,
                thumb=thumb
            )

        else:
            await handle_flood(
                client.copy_message,
                chat_id=chat_id,
                from_chat_id=message.chat.id,
                message_id=message.id
            )

        return True

    except Exception as e:
        logger.error(f"Forward failed {message.id} -> {chat_id}: {e}")
        return False

# ================= BUFFER FORWARD =================
async def forward_buffered_messages(client, messages, chat_id):
    success = 0
    for msg in sorted(messages, key=lambda m: m.id):
        if await forward_single_message(client, msg, chat_id):
            success += 1
        await asyncio.sleep(MSG_DELAY)
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
            await message_queue.put((payload, failed, ftype))

        message_queue.task_done()
        await asyncio.sleep(TARGET_DELAY)

# ================= START WORKERS =================
async def start_processor(client):
    for _ in range(QUEUE_WORKERS):
        asyncio.create_task(process_queue(client))
    logger.info("Queue workers started")

# ================= BUFFER HANDLER =================
async def process_buffered_messages(source_chat_id):
    await asyncio.sleep(BUFFER_DELAY)
    messages = message_buffer.get(source_chat_id)
    if not messages:
        return

    mappings = await database.get_all_targets_for_source(source_chat_id) or []

if not mappings:
    logger.warning(f"No targets found for source {source_chat_id}")
    return

for mapping in mappings:
    targets = mapping.get("target_ids", [])
    if targets:
        await message_queue.put((messages.copy(), targets, "buffered"))
        targets = mapping.get("target_ids", [])
        if targets:
            await message_queue.put((messages.copy(), targets, "buffered"))

    message_buffer.pop(source_chat_id, None)
    buffer_tasks.pop(source_chat_id, None)

# ================= MESSAGE LISTENER =================
@Client.on_message(
    filters.channel &
    (filters.video | filters.document | filters.photo |
     filters.audio | filters.sticker | filters.animation | filters.text)
)
async def forward_content(client, message):
    cid = message.chat.id
    message_buffer[cid].append(message)

    if cid in buffer_tasks:
        buffer_tasks[cid].cancel()

    buffer_tasks[cid] = asyncio.create_task(
        process_buffered_messages(cid)
    )
