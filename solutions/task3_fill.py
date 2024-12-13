from solutions.config import get_db, create_dir, save_json, remove_mongo_id
import msgpack

with open("../data/task_3_item.msgpack", "rb") as file:
    unpacked = msgpack.unpackb(file.read())

print(len(unpacked))

db = get_db()
collection = db['task1']

for item in unpacked:
    collection.insert_one(item)
