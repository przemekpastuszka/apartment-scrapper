# coding=UTF-8
import re
import traceback
import urlparse
from datetime import date as dt

import pandas
import requests
from dateutil.relativedelta import relativedelta

from utils import to_soup, as_int


def stringify_child(child):
    if child.string:
        return unicode(child.string.strip())
    if hasattr(child, 'children'):
        children = list(child.children)
        if len(children) > 1:
            return [stringify_child(x) for x in children]
        if len(children) == 1:
            return stringify_child(children[0])


def table_into_json(table):
    result = []
    trs = table.find_all('tr')
    for tr in trs:
        tds = tr.find_all('td')
        if len(tds) > 1:
            result.append([stringify_child(td) for td in tds])
        if len(tds) == 1:
            result.append(stringify_child(tds[0]))
    return result


def as_float(text):
    if text:
        return float(re.sub("[^0-9\,\.]", "", text).replace(",", "."))
    return None


def as_date(text):
    quarter = int(text[0])
    year = int(text[-4:])
    return dt(year, quarter * 3, 1) + relativedelta(months=1)


def parse_invest_html(url):
    try:
        soup = to_soup(url)
        body = soup.find_all('div', {"data-config-table-container": "propertyListFull"})

        links = []
        if body:
            for row in body[0].tbody.find_all('tr'):
                potential_links = row.find_all('a', href=True)
                if len(potential_links) == 2:
                    links.append(make_link(potential_links[1]))
                else:
                    links.append(make_link(potential_links[0]))
        return links
    except Exception, e:
        raise Exception("Failed to fetch %s; %s" % (url, traceback.format_exc()), e)


def make_link(link):
    return urlparse.urljoin("https://rynekpierwotny.pl/", link['href'])


def parse_search_page(page, region=11158):
    url = ("https://rynekpierwotny.pl/oferty/?type=&region={region}"
           "&distance=0&price_0=&price_1=&area_0=&area_1=&rooms_0=&rooms_1="
           "&construction_end_date=&price_m2_0=&price_m2_1=&floor_0=&floor_1="
           "&offer_size=&keywords=&is_luxury=&page={page}&is_mdm=&is_holiday=&lat=&lng=&sort=").format(
            region=region,
            page=page
    )
    soup = to_soup(url)
    links = []
    for result in soup.find_all('h2', {'class': 'offer-item-name'}):
        links.append(make_link(result.a))
    return links


def search_all_investments(region=11158):
    i = 1
    results = []
    partial_results = parse_search_page(i, region)
    while partial_results:
        results += partial_results
        i += 1
        partial_results = parse_search_page(i, region)
    return results


def as_dict(table):
    result = {}
    for x in table:
        if len(x) == 2:
            key, value = x
            if isinstance(key, list):
                key = key[1]
            result[key] = value
    return result


def retrieve_meta(soup, id, tag_name='id'):
    metas = soup.find_all('meta', {tag_name: id})
    if metas:
        return metas[0]['content']
    return None


def parse_parking_place(str):
    str_no_whitespace = re.sub("\s|\.", "", str)
    ranges = [as_float(x) for x in re.findall("[0-9]+", str_no_whitespace)]
    ranges = [x for x in ranges if x >= 1000]
    if ranges:
        return min(ranges)
    return None


def parse_parking_places(l):
    parsed_places = [parse_parking_place(x) for x in l] if isinstance(l, list) else [
        parse_parking_place(l)]
    parsed_places = [place for place in parsed_places if place]
    if parsed_places:
        return min(parsed_places)
    return None


def one_of(d, keys):
    for key in keys:
        if key in d:
            return d[key]


def parse_details_html(url):
    try:
        soup = to_soup(url)

        basic_data = as_dict(
                table_into_json(soup.find_all('section', {'id': 'szczegoly-oferty'})[0].table))
        extended_data_sections = soup.find_all('section', {'id': 'dodatkowe-oplaty'})
        extended_data = {}
        if extended_data_sections:
            extended_data = as_dict(table_into_json(extended_data_sections[0].table))

        return {
            u"Link": url,
            u"Region": retrieve_meta(soup, 'dimension-region'),
            u"Ulica": retrieve_meta(soup, "streetAddress", "itemprop").lower().strip("ul.").strip(),
            u"Cena mieszkania": as_int(retrieve_meta(soup, 'dimension-price')),
            u"Cena za metr": as_int(retrieve_meta(soup, 'dimension-price-m2')),
            u"Powierzchnia": as_float(retrieve_meta(soup, 'dimension-area')),
            u"Pokoje": as_int(retrieve_meta(soup, 'dimension-rooms')),
            u'Cena parkingu': parse_parking_places(basic_data['Miejsca postojowe:'][1]),
            u'Piętro': as_int(retrieve_meta(soup, 'dimension-floor')),
            u"Koszty dodatkowe": sum([as_int(value) for value in extended_data.values()]) or None,
            u"Długosć geograficzna": as_float(retrieve_meta(soup, 'longitude', 'itemprop')),
            u"Szerokość geograficzna": as_float(retrieve_meta(soup, 'latitude', 'itemprop')),
            u"Termin": as_date(
                    one_of(basic_data, ['Realizacja inwestycji:', u'Realizacja nieruchomości:'])[
                    -16:-6])
        }
    except requests.exceptions.ConnectionError:
        return url
    except Exception, e:
        raise Exception("Failed to fetch %s; %s" % (url, traceback.format_exc()), e)


def flatten_list(l):
    return [item for sublist in l for item in sublist]


def scrap():
    from multiprocessing import Pool
    pool = Pool(30)

    investments = search_all_investments()
    print "There are %s investments found" % len(investments)

    apartments = flatten_list(pool.map(parse_invest_html, investments))
    apartments_details = []
    while apartments:
        print "Fetching %s apartaments" % len(apartments)
        results = pool.map(parse_details_html, apartments)
        apartments_details += [result for result in results if isinstance(result, dict)]
        apartments = [result for result in results if
                      isinstance(result, unicode) or isinstance(result, str)]

    df = pandas.DataFrame(apartments_details)
    df.to_pickle("/home/ppastuszka/Dokumenty/apartmentdata.pkl")


scrap()
# print parse_details_html(
#     "https://rynekpierwotny.pl/oferty/cordia-member-of-futureal/cordia-cystersow-garden-krakow-grzegorzki/347828/")
