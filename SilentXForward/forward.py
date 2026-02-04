import asyncio
import logging
from collections import defaultdict
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError
from SilentXForward import database

# ================= CONFIG =================
BUFFER_DELAY = 2              # seconds to wait for batch
QUEUE_WORKERS = 3             # parallel queue processors
TARGET_CONCURRENCY = 3        # parallel target forwards
MSG_DELAY = 0.1               # delay between files
TARGET_DELAY = 0.15           # delay between batches
# ==========================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

message_queue = asyncio.Queue()
message_buffer = defaultdict(list)
buffer_tasks = {}

# ================= FLOOD HANDLER =================
async def handle_flood(func, **kwargs):
    for attempt in range(3):
        try:
            return await func(**kwargs)
        except FloodWait as e:
            logger.warning(f"FloodWait {e.value}s")
            await asyncio.sleep(e.value + 1)
        except RPCError as e:
            logger.error(f"RPCError: {e}")
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            await asyncio.sleep(2 ** attempt)
    raise Exception("Max retries exceeded")

# ================= SINGLE MESSAGE FORWARD =================
async def forward_single_message(client, message, chat_id):
    try:
        await handle_flood(
            client.copy_message,
            chat_id=chat_id,
            from_chat_id=message.chat.id,
            message_id=message.id,
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

    logger.info(f"Buffered {success}/{len(messages)} -> {chat_id}")
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

        tasks = [forward_target(t, payload, ftype) for t in targets]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for t, r in zip(targets, results):
            if r is not True:
                failed.append(t)

        if failed:
            logger.warning(f"Retrying {len(failed)} targets")
            await message_queue.put((payload, failed, ftype))

        message_queue.task_done()
        await asyncio.sleep(TARGET_DELAY)

# ================= START WORKERS =================
async def start_processor(client):
    for i in range(QUEUE_WORKERS):
        asyncio.create_task(process_queue(client))
    logger.info(f"{QUEUE_WORKERS} queue workers started")

# ================= BUFFER PROCESSOR =================
async def process_buffered_messages(source_chat_id):
    while True:
        await asyncio.sleep(BUFFER_DELAY)

        messages = message_buffer.get(source_chat_id)
        if not messages:
            break

        try:
            mappings = await database.get_all_targets_for_source(source_chat_id)
            for mapping in mappings:
                targets = mapping.get("target_ids", [])
                if targets:
                    await message_queue.put(
                        (messages.copy(), targets, "buffered")
                    )
                    logger.info(
                        f"Queued {len(messages)} msgs from {source_chat_id} -> {len(targets)} targets"
                    )
        except Exception as e:
            logger.error(f"Buffer error: {e}")
        finally:
            message_buffer[source_chat_id].clear()

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

        # IMPORTANT: cancel nahi karte (no random skip)
        if cid not in buffer_tasks:
            buffer_tasks[cid] = asyncio.create_task(
                process_buffered_messages(cid)
            )
    except Exception:
        logger.exception("Handler error")
