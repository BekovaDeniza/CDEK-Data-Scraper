from base import BaseSpider
from validation import validate_response
from utils import consolidate_delivery_routes, write_to_excel, json_validator
from grab.spider import Task

from urllib.parse import quote
import logging
import re
import json


class Parser(BaseSpider):
    base_url = 'https://www.cdek.ru'
    valid_hosts = ['cdek.ru']
    validation_text = "cdek"

    initial_urls = [
        'https://www.cdek.ru/api-lkfl/cities/autocomplete?str={}&page=1&perPage=10'
    ]

    citi_api_url = 'https://www.cdek.ru/api-lkfl/cities/autocomplete?str={}&page=1&perPage=10'
    estimate_url = 'https://www.cdek.ru/api-lkfl/estimateV2'
    tariff_url = 'https://www.cdek.ru/api-lkfl/getTariffInfo'

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
    def get_tariff_json(data, services):
        json_data = {
            "withoutAdditionalServices": 0,
            "serviceId": data['service_id'],
            "mode": data['delivery_mode'],
            "payerType": "sender",
            "currencyMark": "RUB",
            "senderCityId": data['sender_uuid'],
            "receiverCityId": data['receive_uuid'],
            "packages": [{"height": 1, "length": 1, "width": 1, "weight": data['weight']}],
            "additionalServices": []
        }
        if services:
            json_data['additionalServices'] = services

        return json.dumps(json_data)

    def add_task(self, task, *args, **kwargs):
        if task.name == 'initial' and 'str={}' in task.url:
            cities_keys = list(self.cities_uuids.keys())
            citi_name = cities_keys.pop(0)

            task.url = self.citi_api_url.format(quote(citi_name, safe=''))
            task.cities_keys = cities_keys
            task.current_citi_name = citi_name

        super(Parser, self).add_task(task, **kwargs)

    def setup_grab_for_task(self, task):
        g = super(Parser, self).setup_grab_for_task(task)

        if task.name == 'tariff':
            item = task.get('item')
            sender_city = item['sender_city']
            receive_city = item['receive_city']
            service_id = task.get('service_id')
            weight = item['weight']
            services = task.get('services')

            sender_uuid = self.cities_uuids[sender_city]
            receive_uuid = self.cities_uuids[receive_city]

            json_data = self.get_tariff_json({
                'service_id': service_id,
                'delivery_mode': self.delivery_mode,
                'sender_uuid': sender_uuid,
                'receive_uuid': receive_uuid,
                'weight': weight
            }, services)

            g.setup(
                method='POST',
                post=json_data
            )

        g.config['headers'].update({
            'Accept': 'application/json',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Origin': 'https://www.cdek.ru'
        })

        return g

    @validate_response(json_validator)
    def task_initial(self, grab, task):
        cities = grab.doc.json['data']

        cities_keys = task.get('cities_keys')
        current_citi_name = task.get('current_citi_name')
        citi_uuid = None

        for citi in cities:
            if self.clear_name(citi.get('name', '')) == self.clear_name(current_citi_name) and citi.get('uuid'):
                citi_uuid = citi.get('uuid')
                self.cities_uuids[current_citi_name] = citi_uuid
                logging.info(f'Complete parse UUID for {current_citi_name} - {citi_uuid}.')
                break

        if not citi_uuid:
            logging.warning(f'Cities not found in url: {task.url}.')
        else:
            if cities_keys:
                # если у нас собраны uuid не для всех городов, повторяем запрос для следующего города
                citi_name = cities_keys.pop(0)
                url = self.citi_api_url.format(quote(citi_name, safe=''))

                yield Task('initial', url=url, priority=50, cities_keys=cities_keys, current_citi_name=citi_name)
            else:
                # делаем запрос на следующую страницу, если собраны все uuid городов
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
                                "height": 1, "length": 1, "width": 1, "weight": weight
                            }]
                        }

                        g = self.create_grab_instance()
                        g.setup(
                            url=self.estimate_url,
                            method='POST',
                            post=json.dumps(json_data)
                        )

                        item = {
                            'sender_city': sender_city,
                            'receive_city': receive_city,
                            'weight': weight
                        }
                        yield Task('estimate', grab=g, priority=40, item=item)

    @validate_response(json_validator)
    def task_estimate(self, grab, task):
        data = grab.doc.json['data']
        item = task.get('item')

        service_id = None
        max_days = None
        min_days = None
        for delivery in data:
            if self.delivery_desc in delivery.get('description', '').lower():
                for tariff in delivery.get('tariffs', []):
                    if tariff.get('mode', '') == self.delivery_mode:
                        service_id = tariff.get('serviceId')
                        max_days = tariff.get('durationMin')
                        min_days = tariff.get('durationMax')
                        break

        if not service_id:
            logging.warning(f"Service Id not found. sender: {item['sender_city']}. receive: {item['receive_city']}. "
                            f"weight: {item['weight']}")
        else:
            item.update({
                'max_days': max_days,
                'min_days': min_days,
            })

            yield Task('tariff', url=self.tariff_url, priority=20, item=item, service_id=service_id)

    @validate_response(json_validator)
    def task_tariff(self, grab, task):
        data = grab.doc.json['data']
        item = task.get('item')

        if task.get('flag', True):
            # собираем данные о страховке и упаковке, если они не указаны в запросе
            insurance = {}
            package = {}
            for service in data.get('availableAdditionalServices', []):
                if service.get('alias') == 'insurance':
                    insurance = {
                        "alias": "insurance",
                        "arguments": service['arguments']
                    }
                elif 'Box' in service.get('alias') and not package:
                    package = {
                        "alias": service['alias'],
                        "arguments": service['arguments']
                    }
            yield Task('tariff', url=self.tariff_url, priority=20, item=item,
                       services=[insurance, package], service_id=task.get('service_id'), flag=False)
        else:
            # если в запросе учтены страховка и упаковка, собираем цену
            price = data.get('totalCost')

            item.update({'price': price})

            # записываем данные в список словарей
            self.results.append(item)

            logging.info(f"Tariff save. sender: {item['sender_city']}. receive: {item['receive_city']}. "
                         f"weight: {item['weight']}. TotalPrice: {price}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    bot = Parser(thread_number=2)
    bot.run()
    routes_result = consolidate_delivery_routes(bot.results)
    write_to_excel(routes_result)
