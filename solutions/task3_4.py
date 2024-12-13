from solutions.config import get_db, create_dir, save_json, remove_mongo_id

db = get_db()
collection = db['task1']

collection.update_many(
    {"city": {"$in": ['Москва', 'Хинон']}},
    {"$mul": {"salary": 1.07}}
)
