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
            logger.error(f"Unexpected error: {e}")
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

        if message.caption:
            kwargs["caption"] = message.caption
            if message.caption_entities:
                kwargs["caption_entities"] = message.caption_entities

        await handle_flood(client.copy_message, **kwargs)
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
    tasks = []
    for i in range(QUEUE_WORKERS):
        tasks.append(asyncio.create_task(process_queue(client)))
    logger.info(f"{QUEUE_WORKERS} queue workers started")
    return tasks

# ================= BUFFER HANDLER =================
async def process_buffered_messages(source_chat_id):
    await asyncio.sleep(BUFFER_DELAY)

    messages = message_buffer.get(source_chat_id)
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
    except Exception as e:
        logger.error(f"Buffer process error: {e}")
    finally:
        message_buffer.pop(source_chat_id, None)
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

        if cid in buffer_tasks:
            buffer_tasks[cid].cancel()

        buffer_tasks[cid] = asyncio.create_task(
            process_buffered_messages(cid)
        )
    except Exception as e:
        logger.error("Handler error", exc_info=True)
