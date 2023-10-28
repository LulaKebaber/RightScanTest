import re
import json
from datetime import datetime

'''
Уже ненужный класс, так как я изначально предполагал, что входные данные
будут в ввиде Необходимо посчитать суммы всех выплат с {28.02.2022} по {31.03.2022},
единица группировки - {день}. Но в итоге после ответов бота, я понял, что
входные данные будут в формате JSON
'''
class DatePreparator:
    def __init__(self, message):
        self.message = message

    # Нужная функция для разбора JSON
    def json_reader(self):
        # Разбор JSON
        data = json.loads(self.message)

        dt_from = data["dt_from"]
        dt_upto = data["dt_upto"]
        group_type = data["group_type"]

        return dt_from, dt_upto, group_type
    
    # Ненужный функция (1)
    def prepare_dates(self):
        dates = re.findall(r'\{(\d{2}.\d{2}.\d{4})\}', self.message)

        dt_from = datetime.strptime(dates[0], "%d.%m.%Y")  # Начальная дата
        dt_upto = datetime.strptime(dates[1], "%d.%m.%Y")  # Конечная дата

        # Преобразуем даты в формат ISO
        dt_from = dt_from.isoformat()
        dt_upto = dt_upto.isoformat()

        return dt_from, dt_upto
    

    # Ненужный функция (2)
    def prepare_group_type(self):
        group_type = re.search(r'единица группировки - \{(.+?)\}', self.message).group(1)
        group_type_dict = {
            "день": "day",
            "месяц": "month",
            "час": "hour"
        }
        group_type = group_type_dict.get(group_type.lower())

        return group_type