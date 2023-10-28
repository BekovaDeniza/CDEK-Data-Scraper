import pandas as pd
from collections import defaultdict


def json_validator(grab, task=None):
    try:
        _ = grab.doc.json
        return True
    except (ValueError, IndexError, KeyError):
        return False


def consolidate_delivery_routes(routes):
    # Создаем словарь для хранения уникальных маршрутов и соответствующих значений
    consolidated_routes = defaultdict(
        lambda: {'sender_city': '', 'receive_city': '', 'max_days': float('-inf'), 'min_days': float('inf'),
                 'price': {}})

    # Обработка списка и формирование нового словаря
    for route in routes:
        key = (route['sender_city'], route['receive_city'])

        # Проверка и обновление значений в соответствии с условиями
        if route['max_days'] > consolidated_routes[key]['max_days']:
            consolidated_routes[key]['max_days'] = route['max_days']
        if route['min_days'] < consolidated_routes[key]['min_days']:
            consolidated_routes[key]['min_days'] = route['min_days']

        consolidated_routes[key]['sender_city'] = route['sender_city']
        consolidated_routes[key]['receive_city'] = route['receive_city']
        consolidated_routes[key]['price'][route['weight']] = route['price']

    # Формирование итогового списка словарей
    final_routes = []
    for key, value in consolidated_routes.items():
        route = value
        prices = route.pop('price')
        for weight, price in prices.items():
            route[str(weight)] = price
        final_routes.append(route)

    return final_routes


def write_to_excel(routes):
    # Преобразование списка словарей в DataFrame
    df = pd.DataFrame(routes)

    # Переименование заголовков
    column_mapping = {
        "sender_city": "Город (Отправитель)",
        "receive_city": "Город (Получатель)",
        "min_days": "от",
        "max_days": "до"
    }
    df.rename(columns=column_mapping, inplace=True)

    # Переупорядочивание столбцов
    columns_order = ["Город (Отправитель)", "Город (Получатель)", "0.5", "1", "2", "3", "4", "5", "20", "от", "до"]
    df = df.reindex(columns=columns_order)

    # Запись DataFrame в Excel
    df.to_excel("reports/CDEK.xlsx", index=False)
