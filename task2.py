from typing import List
from master import Emitter
from typing import Any


def map(_: str, doc: Any, emit: Emitter):
    emit(doc['product_id'], doc['quantity'])


def reduce(key: str, values: List[Any], emit: Emitter):
    totalQuantity = 0
    for value in values:
        totalQuantity += value
    emit(key, totalQuantity)
