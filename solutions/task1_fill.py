import pickle as pkl
from pymongo import MongoClient

from solutions.config import get_db

with open('../data/task_1_item.pkl', 'rb') as file:
    data = pkl.load(file)

db = get_db()
collection = db['task1']

for item in data:
    collection.insert_one(item)
