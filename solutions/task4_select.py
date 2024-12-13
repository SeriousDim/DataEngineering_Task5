from solutions.config import create_dir, get_db, remove_mongo_id, save_json

db = get_db()
collection = db['task4']

create_dir('../outputs/task4')

results = list(collection.find({'works': {'$gte': 5}}).limit(10))
results = remove_mongo_id(results)
save_json('task4/select1.json', results)

results = list(collection.find({'useful': {'$gte': 5}}).limit(10))
results = remove_mongo_id(results)
save_json('task4/select2.json', results)

results = list(collection.find({'harmful': {'$lte': 5}}).limit(10))
results = remove_mongo_id(results)
save_json('task4/select3.json', results)

results = list(collection.find({'inventor': {'$regex': '^Д-р'}}).limit(10))
results = remove_mongo_id(results)
save_json('task4/select4.json', results)

results = list(collection.find({'$or': [
    {'usability_type': 'Климат'},
    {'harm_type': 'Климат'},
]}).limit(10))
results = remove_mongo_id(results)
save_json('task4/select5.json', results)
