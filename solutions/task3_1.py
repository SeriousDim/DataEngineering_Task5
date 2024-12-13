from solutions.config import get_db, create_dir, save_json, remove_mongo_id

db = get_db()
collection = db['task1']

create_dir('../outputs/task3')

# удалить из коллекции документы по предикату: salary < 25 000 || salary > 175000
result = collection.delete_many({'$or': [
    {'salary': {'$lt': 25000}},
    {'salary': {'gt': 175000}}
]})
print(result.deleted_count)
