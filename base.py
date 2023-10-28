from IPy import IP
from furl import furl
from grab.spider import Spider


class BaseSpider(Spider):
    task_to_ignore_check_domains = []

    valid_hosts = []

    validation_text = None

    def create_grab_instance(self, **kwargs):
        # Переименование переменной для более информативного понимания ее предназначения
        grab_instance = super(BaseSpider, self).create_grab_instance(**kwargs)
        grab_instance.setup(timeout=20)
        return grab_instance

    def check_valid_domain(self, task, url):
        if task.name in self.task_to_ignore_check_domains:
            return True

        hostname = furl(url).host

        try:
            IP(hostname)
        except ValueError:
            # Если возникает ошибка ValueError, следует лучше обработать эту ситуацию
            # Получение последних двух частей имени хоста для сравнения
            real_host = '.'.join(hostname.split('.')[-2:])
            is_valid_host = (real_host in self.valid_hosts)

            if not is_valid_host:
                return False

        return True
