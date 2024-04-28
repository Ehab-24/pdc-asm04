from collections.abc import Callable
from typing import List, Any

from pymongo.collection import Collection

Emitter = Callable[[str, Any], None]
MapFunc = Callable[[str, str, Emitter], None]
ReduceFunc = Callable[[str, List[str], Emitter], None]


class Config:

    def __init__(self, collection: Collection, output_filename: str, num_mappers: int, num_reducers: int, sort: bool = True):
        self.output_filename = output_filename
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.sort = sort
        self.collection = collection


def map_reduce(config: Config, map: MapFunc, reduce: ReduceFunc):

    documents = config.collection.find()

    # 1. Map
    map_output = dict()
    def map_emit(key: str, value: str):
        if key not in map_output:
            map_output[key] = []
        map_output[key].append(value)

    for document in documents:
        map(document['_id'], document, map_emit)

    # 2. Sort in desceding order
    if config.sort:
        for key in map_output:
            map_output[key].sort(reverse=True)

    # 3. Reduce
    reduce_output = []
    def reduce_emit(key: str, value: str):
        reduce_output.append((key, value))

    for key, values in map_output.items():
        reduce(key, values, reduce_emit)

    # 4. Sort reduce output by value
    if config.sort:
        reduce_output.sort(key=lambda x: x[1], reverse=True)

    # 5. Output
    with open(config.output_filename, 'w') as f:
        for key, value in reduce_output:
            f.write(f'{key}\t{value}\n')

    for key, value in reduce_output:
        print(f'{key}\t{value}')
