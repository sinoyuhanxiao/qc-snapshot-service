# db/mongo.py
from pymongo import MongoClient
from config.db_config import MONGO_URI

def get_mongo_db():
    client = MongoClient(MONGO_URI)
    return client.get_database()  # defaults to "dev-mes-qc" from URI