from solutions.config import get_db, create_dir, save_json, remove_mongo_id

db = get_db()
collection = db['task1']

create_dir('../outputs/task1')

results = list(collection.find().sort('salary', -1).limit(10))
results = remove_mongo_id(results)
save_json('task1/salary_desc.json', results)

results = list(collection
           .find({'age': {'$lt': 30}})
           .sort('salary', -1)
           .limit(15)
           )
results = remove_mongo_id(results)
save_json('task1/age_and_salary.json', results)

results = list(collection
           .find({
                'city': 'Мадрид',
                'job': {'$in': ['Архитектор', 'Психолог', 'Строитель']}
            })
           .sort('age')
           .limit(10)
           )
results = remove_mongo_id(results)
save_json('task1/complex_predicate.json', results)

results = list(collection
           .find({
                'age': {'$gte': 20, '$lte': 35},
                'year': {'$gte': 2019, '$lte': 2022},
                '$or': [
                    {'salary': {'$gt': 50000, '$lte': 75000}},
                    {'salary': {'$gt': 125000, '$lt': 150000}}
                ]
            })
           )
results = remove_mongo_id(results)
save_json('task1/last.json', results)
