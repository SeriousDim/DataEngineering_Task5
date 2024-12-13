from solutions.config import create_dir, get_db, remove_mongo_id, save_json
from datetime import datetime

db = get_db()
collection = db['task4']

create_dir('../outputs/task4')

collection.update_many(
    {
        'harmful': {'$gte': 5}
    },
    {
        '$set': {
            'harmful': 10
        }
    }
)

collection.update_many(
    {
        'useful': {'$lte': 5}
    },
    {
        '$set': {
            'useful': 2
        }
    }
)

docs = collection.find()
for d in docs:
    date = d['deployed']
    year = date.split('.')[2]

    if year:
        collection.update_one(
            {'_id': d['_id']},
            {'$set': {'year': year}}
        )

docs = collection.find()
for d in docs:
    metric = d['works'] + d['useful'] - d['harmful']

    collection.update_one(
        {'_id': d['_id']},
        {'$set': {'metric': metric}}
    )

collection.delete_many({
    'metric': {'$lte': 0}
})
