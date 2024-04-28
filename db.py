from typing import Union
from pymongo import MongoClient
import os
#
# from pymongo.database import Database
#
#
# DB: Union[Database, None] = None
# client: Union[MongoClient, None] = None
#
#
# def init():
#     client = MongoClient('localhost', 27017)
#     DB_NAME = os.getenv('DB_NAME')
#     if DB_NAME is None:
#         raise ValueError('DB_NAME environment variable is not set')
#     DB = client[DB_NAME]
#
# def getDb() -> Database:
#     if DB is None:
#         raise ValueError('DB is not initialized')
#     return DB
#
# def cleanup():
#     if client is not None:
#         client.close()
#
# class DB:
#
#

class DB:
    def __init__(self, db_name: str):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client[db_name]
        self.transactions = self.db['transactions']
        self.customers = self.db['customers']
        self.products = self.db['products']

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

