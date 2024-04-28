
from typing import List
from master import Emitter
from typing import Any


def map(_: str, doc: Any, emit: Emitter):
    metrics = dict(
        amount=doc['quantity']*doc['price'],
        quantity=doc['quantity']
    )
    emit(doc['product_id'], metrics)


def reduce(key: str, values: List[Any], emit: Emitter):
    totalAmount = 0
    totalQuantity = 0
    for value in values:
        totalAmount += value['amount']
        totalQuantity += value['quantity']
    emit(key, dict(amount=totalAmount, quantity=totalQuantity))
