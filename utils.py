import datetime
import re
import traceback
from bs4 import BeautifulSoup

import functools
import requests


def next_weekday(d, weekday):
    days_ahead = weekday - d.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    return d + datetime.timedelta(days_ahead)


MONDAY = 0
SATURDAY = 5
high_traffic_date = next_weekday(datetime.datetime.now(), MONDAY).replace(hour=8, minute=5)
low_traffic_date = next_weekday(datetime.datetime.now(), MONDAY).replace(hour=20, minute=0)

weekend_midday_date = next_weekday(datetime.datetime.now(), SATURDAY).replace(hour=13, minute=0)


def as_int(text):
    if text:
        return int(re.sub("[^0-9]", "", text))
    return None


def to_soup(url):
    page = requests.get(url)
    return BeautifulSoup(page.content, "lxml")


def stringify_child(child):
    if child.string:
        return unicode(child.string.strip())
    if hasattr(child, 'children') and child.children:
        return [stringify_child(x) for x in child.children]


class make_failure_aware(object):
    def __init__(self, target):
        self.target = target
        try:
            functools.update_wrapper(self, target)
        except:
            pass

    def __call__(self, url):
        try:
            return self.target(url)
        except requests.exceptions.ConnectionError:
            return url
        except Exception, e:
            raise Exception("Failed to fetch %s; %s" % (url, traceback.format_exc()), e)


def fetch_all(inputs, method, pool):
    data = []
    while inputs:
        print "Fetching %s urls" % len(inputs)
        results = pool.map(method, inputs)
        data += [result for result in results if isinstance(result, dict)]
        inputs = [result for result in results if not isinstance(result, dict)]
    return data
