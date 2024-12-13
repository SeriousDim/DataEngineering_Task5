from solutions.config import get_db, create_dir, save_json, remove_mongo_id

db = get_db()
collection = db['task1']

create_dir('../outputs/task2')

# вывод минимальной, средней, максимальной salary
results = list(collection
           .aggregate([{
                '$group': {
                    '_id': None,
                    'min': {'$min': '$salary'},
                    'max': {'$max': '$salary'},
                    'mean': {'$avg': '$salary'},
                }
            }])
           )
results = remove_mongo_id(results)
save_json('task2/salary.json', results)

# вывод количества данных по представленным профессиям
results = list(collection
           .aggregate([{
                '$group': {
                    '_id': '$job',
                    'count': {'$sum': 1},
                }
            }, {
                '$project': {'job': '$_id', 'count': 1}
            }])
           )
results = remove_mongo_id(results)
save_json('task2/jobs.json', results)

# вывод минимальной, средней, максимальной salary по городу
results = list(collection
           .aggregate([{
                '$group': {
                    '_id': '$city',
                    'min': {'$min': '$salary'},
                    'max': {'$max': '$salary'},
                    'mean': {'$avg': '$salary'},
                }
            }, {
                '$project': {
                    'city': '$_id',
                    'min': 1,
                    'max': 1,
                    'mean': 1,
                }
            }])
           )
results = remove_mongo_id(results)
save_json('task2/salaries_by_city.json', results)

# вывод минимальной, средней, максимальной salary по профессии
results = list(collection
           .aggregate([{
                '$group': {
                    '_id': '$job',
                    'min': {'$min': '$salary'},
                    'max': {'$max': '$salary'},
                    'mean': {'$avg': '$salary'},
                }
            }, {
                '$project': {
                    'job': '$_id',
                    'min': 1,
                    'max': 1,
                    'mean': 1,
                }
            }])
           )
results = remove_mongo_id(results)
save_json('task2/salaries_by_job.json', results)

# вывод минимального, среднего, максимального возраста по городу
results = list(collection
           .aggregate([{
                '$group': {
                    '_id': '$city',
                    'min': {'$min': '$age'},
                    'max': {'$max': '$age'},
                    'mean': {'$avg': '$age'},
                }
            }, {
                '$project': {
                    'city': '$_id',
                    'min': 1,
                    'max': 1,
                    'mean': 1,
                }
            }])
           )
results = remove_mongo_id(results)
save_json('task2/ages_by_city.json', results)

# вывод минимального, среднего, максимального возраста по профессии
results = list(collection
           .aggregate([{
                '$group': {
                    '_id': '$job',
                    'min': {'$min': '$age'},
                    'max': {'$max': '$age'},
                    'mean': {'$avg': '$age'},
                }
            }, {
                '$project': {
                    'job': '$_id',
                    'min': 1,
                    'max': 1,
                    'mean': 1,
                }
            }])
           )
results = remove_mongo_id(results)
save_json('task2/ages_by_job.json', results)

# вывод максимальной заработной платы при минимальном возрасте
results = list(collection
           .aggregate([
            {
                '$group': {
                    '_id': None,
                    'max': {'$max': '$age'},
                }
            }])
           )
max_age = results[0]['max']
results = list(collection
               .aggregate([
                    {'$match': {'age': max_age}},
                    {
                        '$group': {
                            '_id': None,
                            'min_salary': {'$min': '$salary'}
                        }
                    }
                ]))
to_json = {
    'max_age': max_age,
    'min_salary': results[0]['min_salary']
}
save_json('task2/min_salary_for_max_age.json', to_json)

# вывод минимального, среднего, максимального возраста по городу, при условии,
# что заработная плата больше 50 000,
# отсортировать вывод по убыванию по полю avg
results = list(collection
           .aggregate([
            {
                '$match': {
                    'salary': {'$gt': 50000}
                }
            },
            {
                '$group': {
                    '_id': '$city',
                    'min': {'$min': '$age'},
                    'max': {'$max': '$age'},
                    'mean': {'$avg': '$age'},
                }
            }, {
                '$sort': {'mean': -1}
            }, {
                '$project': {
                    'city': '$_id',
                    'min': 1,
                    'max': 1,
                    'mean': 1,
                }
            }])
           )
results = remove_mongo_id(results)
save_json('task2/complex_1.json', results)

# вывод минимальной, средней, максимальной salary
# в произвольно заданных диапазонах по городу, профессии,
# и возрасту: 18<age<25 & 50<age<65
results = list(collection
           .aggregate([
            {
                '$match': {
                    'city': {'$in': ['Москва', 'Мадрид']},
                    'job': {'$in': ['Архитектор', 'Психолог', 'Строитель']},
                    '$or': [
                        {'age': {'$gt': 18, '$lt': 25}},
                        {'age': {'$gt': 50, '$lt': 65}},
                    ]
                }
            },
            {
                '$group': {
                    '_id': None,
                    'min': {'$min': '$salary'},
                    'max': {'$max': '$salary'},
                    'mean': {'$avg': '$salary'},
                }
            }, {
                '$project': {
                    'min': 1,
                    'max': 1,
                    'mean': 1,
                }
            }])
           )
results = remove_mongo_id(results)
save_json('task2/complex_2.json', results)

# произвольный запрос
results = list(collection
           .aggregate([
            {
                '$match': {
                    'city': 'Москва',
                    'salary': {'$gte': 100000},
                    'age': {'$lte': 45}
                }
            },
            {
                '$group': {
                    '_id': '$age',
                    'min': {'$min': '$salary'},
                    'max': {'$max': '$salary'},
                    'mean': {'$avg': '$salary'},
                }
            }, {
                '$sort': {'mean': -1}
            }, {
                '$project': {
                    'age': '$_id',
                    'min': 1,
                    'max': 1,
                    'mean': 1,
                }
            }])
           )
results = remove_mongo_id(results)
save_json('task2/custom.json', results)
