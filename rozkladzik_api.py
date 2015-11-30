import datetime

from utils import to_soup, stringify_child, as_int


def get_public_transport_time(gps_from, gps_to, time=datetime.datetime.now()):
    weekday = (int(time.strftime("%w")) + 5) % 6

    url = "http://www.m.rozkladzik.pl/krakow/wyszukiwarka_polaczen.html?" \
          "from={from_x};{from_y}|c|{from_x}|{from_y}&" \
          "to={to_x};{to_y}|c|{to_x}|{to_y}&profile=opt&maxWalkChange=400&minChangeTime=2&time={time}&day={day}".format(
        from_x=gps_from[0], from_y=gps_from[1], to_x=gps_to[0], to_y=gps_to[1], time=time.strftime("%H:%M"), day=weekday)

    soup = to_soup(url)

    times = []
    for sum_row in soup.find_all('div', {'class': 'route_sum_row'}):
        time_td = sum_row.find_all('td', {'class': 'time'})[0]
        times.append(as_int(stringify_child(time_td)[1]))
    return min(times)
