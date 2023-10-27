import urllib
import csv
import logging
from furl import furl
from IPy import IP
from validation import ValidationError
from traceback import format_exception
import six


from grab.spider import Spider, Task


class BaseSpider(Spider):
    task_to_ignore_check_domains = []

    valid_hosts = []

    validation_text = None

    def __init__(self, *args, **kwargs):
        super(BaseSpider, self).__init__(*args, **kwargs)
        self._prepare_requests_left = 0

    def create_grab_instance(self, **kwargs):
        g = super(BaseSpider, self).create_grab_instance(**kwargs)
        g.setup(timeout=20)
        return g

    def check_valid_domain(self, task, url):
        if task.name in self.task_to_ignore_check_domains:
            return True

        hostname = furl(url).host

        try:
            IP(hostname)
        except ValueError:
            real_host = '.'.join(hostname.split('.')[-2:])
            is_valid_host = (real_host in self.valid_hosts)

            if not is_valid_host:
                return False

        return True
