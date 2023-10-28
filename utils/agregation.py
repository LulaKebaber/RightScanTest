import pymongo
import json
from datetime import datetime
from datetime import timedelta

from .data_grouper import DataGrouper
from config_reader import config

# Подключение к кластеру MongoDB
client = pymongo.MongoClient(config.mongo_url.get_secret_value())
db = client[config.db_name.get_secret_value()]
collection = db[config.collection_name.get_secret_value()]


class Aggregator:
    def __init__(self, dates, group_type):
        self.dt_from = dates[0]
        self.dt_upto = dates[1]
        self.group_type = group_type

    
    def aggregate_data(self):

        date_format = {
            "hour": "%Y-%m-%dT%H:%M:%S",
            "day": "%Y-%m-%dT00:00:00",
            "month": "%Y-%m-01T00:00:00"
        }
        
        # Форматируем даты для запроса в MongoDB
        dt_from = datetime.fromisoformat(self.dt_from)
        dt_upto = datetime.fromisoformat(self.dt_upto)

        pipeline = [
            {
                "$match": {
                    "dt": {"$gte": dt_from, "$lte": dt_upto}
                }
            },
            {
                "$group": {
                    "_id": {
                        "$dateToString": {"format": date_format[self.group_type], "date": "$dt"}
                    },
                    "total_value": {"$sum": "$value"}
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ]
        
        result = list(collection.aggregate(pipeline))

        dataset = [item["total_value"] for item in result]
        labels = [item["_id"] for item in result]

        if self.group_type == "hour":            
            data = DataGrouper.group_data_by_hour(dataset, labels, dt_upto)
            return json.dumps(data)

        elif self.group_type == "day":
            data = DataGrouper.group_data_by_day(dataset, labels, dt_from, dt_upto)
            return json.dumps(data)
        elif self.group_type == "month":
            data = {"dataset": dataset, "labels": labels}
            return json.dumps(data)