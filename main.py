import os
import master
import task1, task2, task3
from dotenv import load_dotenv
import db
import argparse


if __name__ == '__main__':
    load_dotenv()

    parser = argparse.ArgumentParser(prog='Suraj Map Reduce', description='Map Reduce for Suraj by Suraj to Suraj', epilog='Enjoy the program! :)')
    parser.add_argument('--task', type=int, help='Task number to run', required=True)
    args = parser.parse_args()

    db_name = os.getenv('DB_NAME')
    if db_name is None:
        raise ValueError('DB_NAME environment variable is not set')
    db = db.DB(db_name)

    config = master.Config(collection=db.transactions, output_filename='output.txt', num_mappers=1, num_reducers=1)
    if args.task == 1:
        mapFunc = task1.map
        reduceFunc = task1.reduce
    elif args.task == 2:
        mapFunc = task2.map
        reduceFunc = task2.reduce
    elif args.task == 3:
        config.sort = False
        mapFunc = task3.map
        reduceFunc = task3.reduce
    else:
        raise ValueError('Invalid task number')

    master.map_reduce(config, mapFunc, reduceFunc)
