from solutions.config import create_dir, get_db, remove_mongo_id, save_json
from datetime import datetime

db = get_db()
collection = db['task4']

create_dir('../outputs/task4')

results = list(collection
           .aggregate([{
                    '$group': {
                        '_id': '$usability_type',
                        'works_sum': {'$sum': '$works'}
                    }
            }, {
                '$project': {
                    'usability_type': '$_id',
                    'works_sum': 1
                }
            }])
           )
results = remove_mongo_id(results)
save_json('task4/aggregate1.json', results)

results = list(collection
           .aggregate([{
                    '$group': {
                        '_id': '$usability_type',
                        'usability_sum': {'$sum': '$useful'}
                    }
            }, {
                '$project': {
                    'usability_type': '$_id',
                    'usability_sum': 1
                }
            }])
           )
results = remove_mongo_id(results)
save_json('task4/aggregate2.json', results)

results = list(collection
           .aggregate([{
                '$match': {
                    'deployed': {'$gt': "01.01.2026"}
                }
            },{
                    '$group': {
                        '_id': '$usability_type',
                        'usability_sum': {'$sum': '$useful'}
                    }
            }, {
                '$project': {
                    'usability_type': '$_id',
                    'usability_sum': 1
                }
            }])
           )
results = remove_mongo_id(results)
save_json('task4/aggregate3.json', results)

results = list(collection
           .aggregate([{
                    '$group': {
                        '_id': None,
                        'harmful_min': {'$min': '$harmful'},
                        'harmful_max': {'$max': '$harmful'},
                        'harmful_avg': {'$avg': '$harmful'},
                        'harmful_sum': {'$sum': '$harmful'},
                    }
            }])
           )
results = remove_mongo_id(results)
save_json('task4/aggregate4.json', results)

results = list(collection
           .aggregate([{
                    '$group': {
                        '_id': '$usability_type',
                        'usability_sum': {'$sum': '$useful'}
                    }
            }, {
                '$sort': {'usability_sum': -1}
            },{
                '$project': {
                    'usability_type': '$_id',
                    'usability_sum': 1
                }
            }])
           )
results = remove_mongo_id(results)
save_json('task4/aggregate5.json', results)
