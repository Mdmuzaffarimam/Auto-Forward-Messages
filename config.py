import os

API_ID = int(os.environ.get("API_ID", "31943015"))
API_HASH = os.environ.get("API_HASH", "dd6325bea0127b18d4558c5cafb38d12")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://mohammadmuzaffarimambaturbari:sHXNxpKZ9PDjyYQr@cluster0.dqjjo.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
DB_NAME = os.environ.get("DB_NAME", "mohammadmuzaffarimambaturbari")

WEB_SERVER = os.environ.get("WEB_SERVER", "True").lower() in ("true", "1", "t")
PORT = int(os.environ.get("PORT", "8080"))
PING_INTERVAL = int(os.environ.get("PING_INTERVAL", "300"))

TG_WORKERS = int(os.environ.get("TG_WORKERS", "4"))

# Your Koyeb/Heroku App Url
# Example : https://yorappurl.koyeb.app/
APP_URL = os.environ.get("APP_URL", None)
