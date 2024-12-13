from pathlib import Path
import json
from pymongo import MongoClient


DB_NAME = 'lab5'


def get_db():
    client = MongoClient('localhost', 27017)
    return client[DB_NAME]


def create_dir(dirname):
    Path(dirname).mkdir(parents=True, exist_ok=True)


def remove_mongo_id(results):
    for d in results:
        del d['_id']
    return list(results)


def save_json(filename, data):
    with open(f'../outputs/{filename}', 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)
