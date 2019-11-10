import csv
import re
import datetime

from pymongo import MongoClient

client = MongoClient()

concerts_db = client['concerts']
concerts_collection = concerts_db['concerts']


def read_data(csv_file, db_name=concerts_db.concerts_collection):
    """
    Загрузить данные в бд из CSV-файла
    """
    data = []
    prox_list = []
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.reader(csvfile)
        for item in reader:
            prox_list.append(item)
        prox_list.pop(0)
        for line in prox_list:
            artist, price, stage, show_date = line
            show_date = show_date.split('.')
            day, month = int(show_date[0]), int(show_date[1])
            date = datetime.datetime(2019, month, day)
            price = int(price)
            data.append({
                'artist': artist,
                'price': price,
                'stage': stage,
                'date': date
            })
    results = db_name.insert_many(data)
    return results.inserted_ids


def find_cheapest(db_name=concerts_db.concerts_collection):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация:
    https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    result = db_name.find().sort(
        [("price", 1)])
    print('\nБилеты по возрастанию цены: \n')
    for item in list(result):
        print(f"{item['price']} руб.: {item['artist']}, "
              f"{item['stage']}, {item['date'].date()}")
    return result


def find_by_name(name, db_name=concerts_db.concerts_collection):
    """
    Найти билеты по имени исполнителя
    (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """
    print(f'\nПо запросу «{name}» найдены билеты:')
    regex = re.compile(r'{}'.format(name), re.IGNORECASE)
    result = db_name.find({'artist': {
        '$regex': regex}}).sort([("price", 1)])
    for item in list(result):
        print(f"{item['price']} руб.: {item['artist']}, "
              f"{item['stage']}, {item['date'].date()}")
    return result


def find_by_date(start_date, end_date,
                 db_name=concerts_db.concerts_collection):
    """
    :param start_date: Дата в формате DD.MM, string
    :param end_date: Дата в формате DD.MM, string
    :param db_name: коллекция mongo,
        по умолчанию – concerts_collection
    :return:
    """
    # Обрабатываем введённые даты
    start_date = start_date.split('.')
    start_day, start_month = (int(start_date[0].lstrip('0')),
                              int(start_date[1]))
    date_start = datetime.datetime(2019, start_month, start_day)

    end_date = end_date.split('.')
    end_day, end_month = (int(end_date[0].lstrip('0')),
                          int(end_date[1]))
    date_end = datetime.datetime(2019, end_month, end_day)

    print('\nВ указанном временном промежутке найдены билеты: \n')
    result = db_name.find(
        {'date': {'$gte': date_start, '$lte': date_end}}).sort(
        [("date", 1)])
    for item in list(result):
        print(f"{item['date'].date()}: концерт «{item['artist']}», "
              f"{item['stage']}, {item['price']} руб.")
    return result


if __name__ == '__main__':
    # Очистка коллекции
    # concerts_db.concerts_collection.drop()

    # Добавляем данные из файла в коллекцию
    read_data('artists.csv')

    # Cортировка по цене
    find_cheapest()

    # Сортировка по имени
    find_by_name('Seconds to')
    find_by_name('ри')
    find_by_name('T-')
    find_by_name('а')

    # Дополнительное задание
    find_by_date('1.07', '30.07')
