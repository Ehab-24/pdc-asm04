from typing import List
from master import Emitter
from typing import Any


def map(_: str, doc: Any, emit: Emitter):
    emit(doc['product_id'], doc['price'] * doc['quantity'])


def reduce(key: str, values: List[Any], emit: Emitter):
    totalPrice = 0
    for value in values:
        totalPrice += value
    emit(key, totalPrice)
