from solutions.config import get_db, create_dir, save_json, remove_mongo_id
import json
import pandas as pd

db = get_db()
collection = db['task4']

df = pd.read_csv('../data/task4.csv', sep=',')
entries = df.to_dict(orient='records')

for item in entries:
    collection.insert_one(item)

with open('../data/task4.json', 'r', encoding='utf-8') as file:
    entries = json.loads(file.read())

for item in entries:
    collection.insert_one(item)
