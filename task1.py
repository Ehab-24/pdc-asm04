from typing import List
from pymongo.command_cursor import CommandCursor
from master import Emitter
from db import DB
from typing import Any


def map(_: str, doc: Any, emit: Emitter):
    for transaction in doc['transactions']:
        revenue = transaction['price'] * transaction['quantity']
        emit(doc['_id'], revenue)


def reduce(key: str, values: List[Any], emit: Emitter):
    totalPrice = 0
    for value in values:
        totalPrice += value
    emit(key, totalPrice)


def generate_docs(db: DB) -> CommandCursor:
    pipeline = [
        {
            '$group': {
                '_id': "$category",
                'products': {
                    '$addToSet': "$$ROOT"
                }
            }
        },
        {
            '$lookup': {
                'from': "transactions",
                'localField': "products.product_id",
                'foreignField': "product_id",
                'as': "transactions"
            }
        }
    ]
    docs = db.products.aggregate(pipeline)
    return docs
