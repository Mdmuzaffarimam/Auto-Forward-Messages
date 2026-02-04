import time
import asyncio
import logging
import threading
import urllib.request

from aiohttp import web
from pyrogram import Client
from SilentXForward import web_server
from config import (
    API_ID,
    API_HASH,
    BOT_TOKEN,
    TG_WORKERS,
    WEB_SERVER,
    PORT,
    APP_URL
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------ PING LOOP (KOYEB SLEEP FIX) ------------------

def ping_loop():
    while True:
        try:
            if APP_URL:
                with urllib.request.urlopen(APP_URL, timeout=10) as r:
                    if r.status == 200:
                        logger.info("✅ Ping successful")
                    else:
                        logger.warning(f"⚠️ Ping status: {r.status}")
        except Exception as e:
            logger.debug(f"Ping error: {e}")
        time.sleep(300)

if APP_URL:
    threading.Thread(target=ping_loop, daemon=True).start()

# ------------------ WEB SERVER ------------------

async def create_server():
    try:
        runner = web.AppRunner(await web_server())
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        logger.info(f"🌐 Web server started on port {PORT}")
    except Exception as e:
        logger.error(f"Web server failed: {e}")

# ------------------ BOT CLASS ------------------

class Bot(Client):
    def __init__(self):
        super().__init__(
            name="SilentXForwardBot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            workers=TG_WORKERS,
            sleep_threshold=10,
            plugins={"root": "SilentXForward"}  # 👈 forwarding yahin se hoga
        )

    async def start(self):
        await super().start()
        me = await self.get_me()
        logger.info(f"🤖 Bot Started: {me.first_name} (@{me.username})")

        if WEB_SERVER:
            await create_server()

    async def stop(self, *args):
        logger.info("🛑 Bot stopping...")
        await super().stop()
        logger.info("✅ Bot stopped cleanly")

# ------------------ RUN ------------------

if __name__ == "__main__":
    Bot().run()
