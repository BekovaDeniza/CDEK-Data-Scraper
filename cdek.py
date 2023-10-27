from grab.spider import Task
import logging
import json
from base import BaseSpider
from validation import validate_response
from furl import furl
import re
from urllib.parse import quote


def json_validator(grab, task=None):
    try:
        _ = grab.doc.json
        return True
    except (ValueError, IndexError, KeyError):
        return False


class Parser(BaseSpider):
    base_url = 'https://www.cdek.ru'
    valid_hosts = ['cdek.ru']
    validation_text = "cdek"

    initial_urls = [
        'https://www.cdek.ru/api-lkfl/cities/autocomplete?str={}&page=1&perPage=10'
    ]

    citi_api_url = 'https://www.cdek.ru/api-lkfl/cities/autocomplete?str={}&page=1&perPage=10'
    estimate_url = 'https://www.cdek.ru/api-lkfl/estimateV2'
    tarif_url = 'https://www.cdek.ru/api-lkfl/getTariffInfo'

    results = []

    @staticmethod
    def get_cities_uuids(delivery_routes):
        # создаем словарь для каждого уникального имени из маршрутов доставки
        route_dict = {}

        for from_city, to_city in delivery_routes:
            if from_city not in route_dict:
                route_dict[from_city] = None
            if to_city not in route_dict:
                route_dict[to_city] = None

        return route_dict

    @staticmethod
    def clear_name(name):
        clear_name = re.sub(r'[^a-zA-Zа-яА-Я]', '', name)
        return clear_name.lower()

    def __init__(self, *args, **kwargs):
        super(Parser, self).__init__(*args, **kwargs)

        # веса доставок
        self.package_weights = [0.5, 1, 2, 3, 4, 5, 20]

        self.delivery_mode = "HOME-HOME"

        self.delivery_desc = "экспресс"

        #  маршруты доставки
        self.delivery_routes = [
            ("Москва", "Москва"),
            ("Москва", "Санкт-Петербург"),
            ("Санкт-Петербург", "Москва"),
            ("Санкт-Петербург", "Санкт-Петербург"),
            ("Москва", "Краснодар"),
            ("Москва", "Екатеринбург"),
            ("Москва", "Новосибирск"),
            ("Москва", "Ростов-На-Дону"),
            ("Москва", "Владивосток"),
            ("Москва", "Нижний Новгород"),
            ("Новосибирск", "Москва"),
            ("Москва", "Казань"),
            ("Екатеринбург", "Москва"),
            ("Москва", "Воронеж"),
            ("Москва", "Сочи"),
            ("Москва", "Хабаровск"),
            ("Краснодар", "Москва"),
            ("Москва", "Калининград"),
            ("Москва", "Самара"),
            ("Москва", "Челябинск"),
            ("Москва", "Красноярск")
        ]

        # словарь для хранения uuid значений городов для запроса
        self.cities_uuids = self.get_cities_uuids(delivery_routes=self.delivery_routes)

    @staticmethod
    def get_tarif_json(data):
        json_data = {
            "withoutAdditionalServices": 0,
            "serviceId": data['service_id'],
            "mode": data['delivery_mode'],
            "payerType": "sender",
            "currencyMark": "RUB",
            "senderCityId": data['sender_uuid'],
            "receiverCityId": data['receive_uuid'],
            "packages": [{"height": 1, "length": 1, "width": 1, "weight": data['weight']}],
            "additionalServices": [{
                "alias": "insurance",
                 "arguments": [{
                     "name": "insurance_declaredCost",
                      "type": "money",
                      "title": "Объявленная стоимость",
                      "placeholder": "Введите сумму",
                      "minValue": 999,
                      "maxValue": 'null',
                      "value": "999"
                 }]
            },
                {
                    "alias": "boxS(2Kilos23x19x10Cm)",
                    "arguments": [{
                        "name": "boxS(2Kilos23x19x10Cm)count",
                        "type": "integer",
                        "title": "Количество",
                        "placeholder": "шт.",
                        "minValue": 'null',
                        "maxValue": 'null',
                        "value": 1
                    }]
                }
            ]
        }

        return json.dumps(json_data)

    def add_task(self, task, *args, **kwargs):
        if task.name == 'initial' and 'str={}' in task.url:
            cities_keys = list(self.cities_uuids.keys())
            citi_name = cities_keys.pop(0)

            task.url = self.citi_api_url.format(quote(citi_name, safe=''))
            task.cities_keys = cities_keys
            task.current_citi_name = citi_name

        super(Parser, self).add_task(task, **kwargs)

    @validate_response(json_validator)
    def task_initial(self, grab, task):
        cities = grab.doc.json['data']

        cities_keys = task.get('cities_keys')
        current_citi_name = task.get('current_citi_name')
        citi_uuid = None

        for citi in cities:
            if self.clear_name(citi.get('name', '')) == self.clear_name(current_citi_name):
                citi_uuid = citi.get('uuid')
                if citi_uuid:
                    self.cities_uuids[current_citi_name] = citi_uuid

        if not cities or not citi_uuid:
            print(f'Cities not found in url: {task.url}.')
        else:
            if cities_keys:
                citi_name = cities_keys.pop(0)
                url = self.citi_api_url.format(quote(citi_name, safe=''))

                yield Task('initial', url=url, priority=50, cities_keys=cities_keys, current_citi_name=citi_name)
            else:
                for sender_city, receive_city in self.delivery_routes:
                    sender_uuid = self.cities_uuids[sender_city]
                    receive_uuid = self.cities_uuids[receive_city]
                    for weight in self.package_weights:
                        json_data = {
                            "payerType": "sender",
                            "currencyMark": "RUB",
                            "senderCityId": sender_uuid,
                            "receiverCityId": receive_uuid,
                            "packages": [{
                                "height":1, "length":1, "width": 1, "weight": weight
                            }]
                        }
                        g = self.create_grab_instance()
                        g.setup(
                            url=self.estimate_url,
                            method='POST',
                            post=json.dumps(json_data),
                            headers={
                              'Accept': 'application/json',
                              'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                              'Cache-Control': 'no-cache',
                              'Connection': 'keep-alive',
                              'Content-Type': 'application/json',
                              'Origin': 'https://www.cdek.ru'
                            }
                        )

                        yield Task('estimate', grab=g, priority=40,
                                   receive_city=receive_city, sender_city=sender_city, weight=weight)

    @validate_response(json_validator)
    def task_estimate(self, grab, task):
        data = grab.doc.json['data']
        service_id = None
        for delivery in data:
            if self.delivery_desc in delivery.get('description', '').lower():
                for tarif in delivery.get('tariffs', []):
                    if tarif.get('mode', '') == self.delivery_mode:
                        service_id = tarif.get('serviceId')
                        break

        if not service_id:
            print(f"Service Id not found. sender: {task.get('sender_uuid')}. receive: {task.get('receive_uuid')}. "
                  f"weight: {task.get('weight')}")
        else:
            sender_city = task.get('sender_city')
            receive_city = task.get('receive_city')
            sender_uuid = self.cities_uuids[sender_city]
            receive_uuid = self.cities_uuids[receive_city]
            json_data = self.get_tarif_json({
                'service_id': service_id,
                'delivery_mode': self.delivery_mode,
                'sender_uuid': sender_uuid,
                'receive_uuid': receive_uuid,
                'weight': task.get('weight')
            })

            item = {
                'max_days': tarif.get('durationMin'),
                'min_days': tarif.get('durationMax'),
                'sender_city': sender_city,
                'receive_uuid': receive_city,
                'weight': task.get('weight')
            }
            g = self.create_grab_instance()
            g.setup(
                url=self.tarif_url,
                method='POST',
                post=json.dumps(json_data),
                headers={
                    'Accept': 'application/json',
                    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                    'Content-Type': 'application/json',
                    'Origin': 'https://www.cdek.ru'
                }
            )
            yield Task('tarif', grab=g, priority=20, item=item)

    @validate_response(json_validator)
    def task_tarif(self, grab, task):
        data = grab.doc.json['data']
        item = task.get('item')

        price = data.get('totalCost')

        item.update({'price': price})
        self.results.append(item)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    bot = Parser(thread_number=1)
    bot.run()
    pass
