from typing import List
from master import Emitter
from typing import Any
from db import DB


def map(_: str, doc: Any, emit: Emitter):
    category = doc['category']
    product_id = doc['product_id']
    quantity = doc['transaction']['quantity']
    emit(f'{category}:{product_id}', quantity)


def reduce(key: str, values: List[Any], emit: Emitter):
    totalQuantity = 0
    for value in values:
        totalQuantity += value
    emit(key, totalQuantity)

def generate_docs(db: DB):
    pipeline = [
        {
            "$lookup": {
                "from": "transactions",
                "localField": "product_id",
                "foreignField": "product_id",
                "as": "transaction"
            }
        },
        {
            "$unwind": "$transaction"
        }
    ]
    return db.products.aggregate(pipeline)
