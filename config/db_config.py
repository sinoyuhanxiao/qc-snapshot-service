import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()
# load_dotenv(dotenv_path=".env.dev") # for running on dev

DB_CONFIG = {
    'host': os.getenv("PG_HOST"),
    'port': int(os.getenv("PG_PORT", "5432")),
    'dbname': os.getenv("PG_DBNAME"),
    'user': os.getenv("PG_USER"),
    'password': os.getenv("PG_PASSWORD"),
}

MONGO_URI = os.getenv("MONGO_URI")