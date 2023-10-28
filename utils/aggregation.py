import pymongo
from datetime import datetime
from datetime import timedelta

# Подключение к кластеру MongoDB
client = pymongo.MongoClient('mongodb+srv://Kebaber:admin@rightscancluster.yrubl8b.mongodb.net/?retryWrites=true&w=majority')
db = client['RightScanDataBase']
collection = db['RightScanCollection']


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

def group_data_by_hour(dataset, labels):
    grouped_data = {}
    
    for i in range(len(labels)):
        label = labels[i]
        hour = label[11:13]  # Извлекаем час из метки времени
        if hour not in grouped_data:
            grouped_data[hour] = 0
        grouped_data[hour] += dataset[i]
    
    grouped_labels = list(grouped_data.keys())
    grouped_dataset = list(grouped_data.values())
    
    return {"dataset": grouped_dataset, "labels": grouped_labels}


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
            return group_data_by_hour(dataset, labels)
        elif self.group_type == "day":
            return group_data_by_day(dataset, labels, dt_from, dt_upto)
        elif self.group_type == "month":
            return {"dataset": dataset, "labels": labels}
