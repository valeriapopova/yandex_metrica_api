import codecs
import csv
import json
import random

import pandas as pd

import requests


from flask import Response

from settings import token


def get_data_key(json_file):
    result = json_file['data']
    data_for_sheets = []
    for key in result:
        for k in key:
            if k not in data_for_sheets:
                data_for_sheets.append(k)
    return data_for_sheets


def to_csv(json_file, counter_id, method):
    with open(f'new/{counter_id}_{method}.csv', 'w') as myfile:
        wr = csv.writer(myfile)
        k = get_data_key(json_file)
        wr.writerow(k)
        for file_ in json_file['data']:
            val = []
            for k, v in file_.items():
                val.append(v)
            wr.writerow(val)

        return f'new/{counter_id}_{method}.csv'


def get_counter(token):
    headers = {
        'oauth_token': token,
        'Authorization': 'OAuth {}'.format(token),
        'Content-Type': 'application/json'
    }
    response = requests.get('https://api-metrika.yandex.ru/management/v1/counters',
                            headers=headers)
    if response.status_code == 200:
        resp_dict = response.json()
        counter_id = resp_dict['counters'][0]['id']
        # pprint(response.json())
        return counter_id


# counter_id = get_counter(token)


def get_goal_id(token, counterId):
    headers = {
        'oauth_token': token,
        'Authorization': 'OAuth {}'.format(token),
        'Content-Type': 'application/json'
    }
    response = requests.get(f'https://api-metrika.yandex.ru/management/v1/counter/{counterId}/goals',
                            headers=headers)
    if response.status_code == 200:
        resp_dict = response.json()
        goal_ids = []
        for i in range(len(resp_dict['goals'])):
            g = resp_dict['goals'][i]['id']
            goal_ids.append(g)
        # pprint(response.json())
        return goal_ids


def get_conversion_goal(token, goal_id, counter_id):
    """Получить конверсии по целям"""
    headers = {
        'oauth_token': token,
        'Authorization': 'OAuth {}'.format(token),
        'Content-Type': 'application/json'
    }

    url = f'https://api-metrika.yandex.net/stat/v1/data?metrics=ym:s:users,ym:s:goal{goal_id}conversionRate&goal_id={goal_id}&id={counter_id}'
    response = requests.get(url, headers=headers)
    return response.json()


# goal_id = get_goal_id(token, get_counter(token))


# g = get_conversion_goal(266841603, get_counter(token))
# print(g)


def post_expense(token, counter_id, file):
    """Загрузка расходов на рекламу"""
    headers = {
        'oauth_token': token,
        'Authorization': 'OAuth {}'.format(token),
        'Content-Type': "text/csv"
    }

    res = requests.post(
        f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/expense/upload',
        headers=headers, data=file)
    return res


json_expences = {"data": [
    {"Date": "2022-10-22",
     "UTMSource": "inst",
     "Expenses": 2000},
    {"Date": "2022-10-23",
     "UTMSource": "inst",
     "Expenses": 7000}]
}

# g = to_csv(json_expences, 90946026)
#
# f = open(g, "rb")
# p = post_expense(token, 90946026, f)
# print(p.json())


def status_order(token, counter_id, file):
    headers = {
        'oauth_token': token,
        'Authorization': 'OAuth {}'.format(token),
        'Content-Type': 'application/json'
    }
    url = f"https://api-metrika.yandex.net/cdp/api/v1/counter/{counter_id}/schema/order_statuses?"
    res = requests.post(url, headers=headers, data=file)
    return res


def post_offline_conv(token, counter_id, client_id_type, file):
    """Загрузка offline конверсий"""
    headers = {
        'oauth_token': token,
        'Authorization': 'OAuth {}'.format(token),
        'Content-Type': "text/csv"
    }

    r = requests.get(
            f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/offline_conversions/'
            f'visit_join_threshold', headers=headers)

    url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/offline_conversions/upload?client_id_type={client_id_type}'
    res = requests.post(url, headers=headers, files={"file":file})
    return res

#
# f = open("new/90745134_offline.csv", "rb")
# p = post_offline_conv(token, 90745134, "CLIENT_ID", f)
# print(p.json())


def post_calls(token, counter_id, client_id_type, file):
    """Загрузка информации о звонках"""
    headers = {
        'oauth_token': token,
        'Authorization': 'OAuth {}'.format(token),
    }
    res = requests.post(f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/offline_conversions/'
                        f'upload_calls?client_id_type={client_id_type}',
                        headers=headers, files={"file": file})
    return res


# f = open("files/calls.csv", "r")
# p = post_calls(token, counter_id, "CLIENT_ID", f)
# print(p.json())


def get_utm_stat(token, goal_id, counter_id):
    """Выгрузка отчета UTM метки по цели"""
    headers = {
        'oauth_token': token,
        'Authorization': 'OAuth {}'.format(token),
        'Content-Type': "Content-Disposition"
    }
    url = f'https://api-metrika.yandex.ru/stat/v1/data.csv?ym:s:UTMSource,ym:s:UTMMedium,ym:s:UTMCampaign,ym:s:UTMContent&metrics=ym:s:users,ym:s:goal{goal_id}conversionRate&goal_id={goal_id}&id={counter_id}'
    r = requests.get(url, headers=headers)
    decoded_data = codecs.decode(r.text.encode(), 'utf-8-sig')
    return decoded_data


def get_uploadings_data(token, counter_id):
    headers = {
        'oauth_token': token,
        'Authorization': 'OAuth {}'.format(token),
        'Content-Type': 'application/json'
    }
    result = requests.get(f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/expense/uploadings',
                          headers=headers)

    return result.json()


def create_client_info(token, counter_id, jsonfile):
    "Новая информация о клиентах добавляется(обновляется) к ранее загруженной."
    headers = {
        'oauth_token': token,
        'Authorization': 'OAuth {}'.format(token),
        "Content-Type": "application/json"
    }

    url = f'https://api-metrika.yandex.net/cdp/api/v1/counter/{counter_id}/data/contacts?merge_mode=APPEND'
    r = requests.post(url, json=jsonfile, headers=headers)
    return r




# atrr = {
#     "contacts":
#         [
#             {
#                 "uniq_id": "J3QQ4-H7H2V-2HCH4-M3HK8-6M8VS",
#                 "name": "Ватрушкин И.М.",
#                 "create_date_time": "2022-10-18 16:12:21",
#                 "update_date_time": "2022-10-18 16:12:21",
#                 "client_ids": ["12345678910", "10987654321"],
#                 "emails": ["exampl1@example.com", "example2@example.com"],
#                 "phones": ["88005553535", "83449932378"]
#
#             },
#             {
#                 "uniq_id": "SSQQ4-H7H2V-2HCH4-M3HK8-6M8VQ",
#                 "name": "Лол И.М.",
#                 "create_date_time": "2022-10-17 16:12:21",
#                 "update_date_time": "2022-10-17 16:12:21",
#                 "client_ids": ["12345678910", "10987654321"],
#                 "emails": ["exampl1@example.com", "example2@example.com"],
#                 "phones": ["88005553535", "83449932378"]
#
#             }
#         ]
# }
#
# a = create_client_info(token, 90745134, atrr)
# print(a.json())


def create_order_info(token, counter_id, jsonfile):
    "Новая информация о заказах добавляется(обновляется) к ранее загруженной."
    headers = {
        'oauth_token': token,
        'Authorization': 'OAuth {}'.format(token),
        "Content-Type": "application/json"
    }

    url = f'https://api-metrika.yandex.net/cdp/api/v1/counter/{counter_id}/data/orders?merge_mode=APPEND'
    r = requests.post(url, json=jsonfile, headers=headers)
    return r


#
# j = {
#     "orders": [
#         {
#             "id": "32152",
#             "client_uniq_id": "J3QQ4-H7H2V-2HCH4-M3HK8-6M8VQ",
#             "client_type": "CONTACT",
#             "create_date_time": "2022-10-18 13:17:00",
#             "update_date_time": "2022-10-18 16:12:21",
#             "finish_date_time": "2022-10-18 23:59:00",
#             "revenue": 1000,
#             "order_status": "id123",
#             "cost": 500,
#             "products": {"Товар А": 173, "Товар Б": 146}
#
#         }
#     ]
# }
#
# h = create_order_info(counter_id, j)
# print(h.json())

def status_orders(token, counter_id, jsonfile):
    "Сопоставление статусов заказов"
    headers = {
        'oauth_token': token,
        'Authorization': 'OAuth {}'.format(token)
    }

    url = f'https://api-metrika.yandex.net/cdp/api/v1/counter/{counter_id}/schema/order_statuses?'
    r = requests.post(url, json=jsonfile, headers=headers)
    return r


j = {
    "order_statuses" : [ {
        "id" : 1,
        "humanized" : "new",
        "type" : "IN_PROGRESS"
    }]
}

# s = status_orders(token, counter_id, j)
# print(s.json())


