# coding: utf-8
import pandas
import json
from datetime import date
from dateutil.relativedelta import relativedelta
import numpy as np

mdm_limit = 5247
mdm_discount = 0.1
monthly_rent = 900

with open("/home/ppastuszka/Dokumenty/apartmentdata", "rb") as f:
    raw_data = json.load(f)
data = pandas.DataFrame(raw_data)

cleaned_data = data[np.isfinite(data['Cena mieszkania'])]
cleaned_data['Cena parkingu'].fillna(0, inplace=True)
cleaned_data['Cena za metr'].fillna(cleaned_data['Cena mieszkania'] / cleaned_data['Powierzchnia'], inplace=True)


def calculate_mdm_discount(row):
    if row['Cena za metr'] <= mdm_limit:
        discounted_area = min(row['Powierzchnia'], 50)
        return discounted_area * mdm_discount * row['Cena za metr']
    return 0


cleaned_data[u"Zniżka MDM"] = cleaned_data.apply(calculate_mdm_discount, axis=1)
cleaned_data[u'Cena mieszkania z MDM'] = cleaned_data['Cena mieszkania'] - cleaned_data[u"Zniżka MDM"]
cleaned_data['Cena mieszkania + parking MDM'] = cleaned_data['Cena mieszkania z MDM'] + cleaned_data['Cena parkingu']
cleaned_data['Termin'] = pandas.to_datetime(cleaned_data['Termin'])


def months_left(to_date):
    delta = relativedelta(to_date, date.today())
    if delta.years >= 0 and delta.months >= 0 and delta.days >= 0:
        return delta.years * 12 + delta.months + (1 if delta.days > 0 else 0)
    return 0


cleaned_data[u'Miesięcy do oddania'] = cleaned_data['Termin'].apply(months_left)
cleaned_data[u'Strata na czynszu'] = cleaned_data[u'Miesięcy do oddania'] * monthly_rent

cleaned_data[u'Cena łączna z MDM'] = cleaned_data['Cena mieszkania + parking MDM'] + cleaned_data['Koszty dodatkowe'] + \
                                     cleaned_data['Strata na czynszu']

cleaned_data[u'Wkład 15%'] = 0.15 * cleaned_data['Cena mieszkania + parking MDM']
cleaned_data[u'Wkład 20%'] = 0.2 * cleaned_data['Cena mieszkania + parking MDM']

with open("/home/ppastuszka/Dokumenty/cleanedapartmentdata.csv", "wb") as f:
    cleaned_data.to_csv(f, encoding='utf-8')