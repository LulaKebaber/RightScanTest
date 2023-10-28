import pymongo
from datetime import datetime
from datetime import timedelta
import json

# Подключение к кластеру MongoDB
client = pymongo.MongoClient('MONGO_URL')
db = client['MONGO_DB']
collection = db['MONGO_COLLECTION']


def group_data_by_day(dataset, labels, dt_from, dt_upto):
    # Создаем список дней в формате "YYYY-MM-DDT00:00:00" в заданном диапазоне
    dt_from = datetime.fromisoformat(str(dt_from))
    dt_upto = datetime.fromisoformat(str(dt_upto))
    current_date = dt_from
    days_list = []

    while current_date <= dt_upto:
        days_list.append(current_date.strftime("%Y-%m-%dT00:00:00"))
        current_date += timedelta(days=1)

    # Создаем словарь, где ключ - день, значение - индекс в исходном списке
    labels_dict = {label: idx for idx, label in enumerate(labels)}

    # Заполняем новый список с нулевыми значениями и добавляем отсутствующие дни
    new_dataset = []
    for label in days_list:
        if label in labels_dict:
            new_dataset.append(dataset[labels_dict[label]])
        else:
            new_dataset.append(0)

    return {"dataset": new_dataset, "labels": days_list}


def group_data_by_hour(dataset, labels, dt_from, dt_upto):
    grouped_data = {}
    
    for i in range(len(labels)):
        label = labels[i]
        year = label[0:4]  # Извлекаем год из метки времени
        month = label[5:7]  # Извлекаем месяц из метки времени
        day = label[8:10]  # Извлекаем день из метки времени
        hour = label[11:13]  # Извлекаем час из метки времени
        if (year, month, day, hour) not in grouped_data:
            grouped_data[(year, month, day, hour)] = 0
        grouped_data[(year, month, day, hour)] += dataset[i]
    
    grouped_labels = list(grouped_data.keys())
    grouped_dataset = list(grouped_data.values())

    formatted_data = {
        "dataset": grouped_dataset,
        "labels": [f"{year}-{month}-{day}T{hour}:00:00" for year, month, day, hour in grouped_labels]
    }

    # проверяем, есть ли в данных пропуски
    if len(formatted_data["dataset"]) < 24:
        for i in range(24):
            if i not in formatted_data["labels"]:
                formatted_data["labels"].insert(i, f"2021-01-01T{i}:00:00")
                formatted_data["dataset"].insert(i, 0)
    
    # проверяем на включение в диапазон второго дня
    if dt_upto.hour == 0:
        formatted_data["labels"].insert(24, f"{year}-{month}-{'0' + str(int(day) + 1)}T00:00:00")
        formatted_data["dataset"].insert(24, 0)
    
    return formatted_data


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
            data = group_data_by_hour(dataset, labels, dt_from, dt_upto)
            return json.dumps(data)

        elif self.group_type == "day":
            data = group_data_by_day(dataset, labels, dt_from, dt_upto)
            return json.dumps(data)
        elif self.group_type == "month":
            data = {"dataset": dataset, "labels": labels}
            return json.dumps(data)
