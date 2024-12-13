from solutions.config import get_db, create_dir, save_json, remove_mongo_id
import pandas as pd

db = get_db()
collection = db['task1']

df = pd.read_csv('../data/task_2_item.csv', sep=';')
entries = df.to_dict(orient='records')

for item in entries:
    collection.insert_one(item)
