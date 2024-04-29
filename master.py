from collections.abc import Callable
from typing import List, Any, Union

from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor


Emitter = Callable[[str, Any], None]
MapFunc = Callable[[str, Any, Emitter], None]
ReduceFunc = Callable[[str, List[str], Emitter], None]
SortFunc = Callable[[str, str], int]


class Config:
    def __init__(self, docs: Union[CommandCursor, Cursor, None], output_filename: str, num_mappers: int, num_reducers: int):
        self.output_filename = output_filename
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.docs = docs


def map_reduce(config: Config, map: MapFunc, reduce: ReduceFunc):

    # 1. Map
    map_output = dict()
    def map_emit(key: str, value: str):
        if key not in map_output:
            map_output[key] = []
        map_output[key].append(value)

    if config.docs is None:
        raise ValueError('No documents to process')

    for doc in config.docs:
        map(str(doc['_id']), doc, map_emit)

    # 2. Reduce
    reduce_output = []
    def reduce_emit(key: str, value: str):
        reduce_output.append((key, value))

    for key, values in map_output.items():
        reduce(key, values, reduce_emit)

    # 3. Sort reduce output by value
    reduce_output.sort(key=lambda x: str(x))

    # 4. Write output to file
    with open(config.output_filename, 'w') as f:
        for key, value in reduce_output:
            f.write(f'{key}\t{value}\n')

    # 5. Print output
    for key, value in reduce_output:
        print(f'{key}\t{value}')
