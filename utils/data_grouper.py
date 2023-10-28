from datetime import datetime, timedelta


class DataGrouper:
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


    def group_data_by_hour(dataset, labels, dt_upto):
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

        # Преобзразуем данные в формат ISO формат
        formatted_data = {
            "dataset": grouped_dataset,
            "labels": [f"{year}-{month}-{day}T{hour}:00:00" for year, month, day, hour in grouped_labels]
        }

        # Проверяем, есть ли в данных пропуски
        if len(formatted_data["dataset"]) < 24:
            for i in range(24):
                if i not in formatted_data["labels"]:
                    formatted_data["labels"].insert(i, f"2021-01-01T{i}:00:00")
                    formatted_data["dataset"].insert(i, 0)
        
        # Проверяем на включение в диапазон второго дня
        if dt_upto.hour == 0:
            formatted_data["labels"].insert(24, f"{year}-{month}-{'0' + str(int(day) + 1)}T00:00:00")
            formatted_data["dataset"].insert(24, 0)
        
        return formatted_data