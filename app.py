import csv
import os
import random

import requests
from flask import Flask, request, Response
from werkzeug.exceptions import BadRequestKeyError

from config import Configuration
from metrica import post_expense, get_counter, get_utm_stat, create_client_info, create_order_info, get_goal_id, \
    get_conversion_goal, post_calls, status_orders, to_csv, get_data_key, post_offline_conv

app = Flask(__name__)
app.config.from_object(Configuration)


@app.route('/yandex')
def homepage():
    return Response('ок', 200)


@app.route('/yandex/metrica/post_expences', methods=['POST'])
def post_expences():
    """Загрузка расходов на рекламу"""
    try:
        json_file = request.get_json(force=False)
        token = json_file['token']
        if 'counter_id' in json_file:
            counter_id = json_file['counter_id']
        else:
            counter_id = get_counter(token)

        filename = to_csv(json_file, counter_id, 'expences')

        f = open(filename, 'rb')

        post_expense(token, counter_id, f)

        return Response("Расходы загружены", 201)

    except BadRequestKeyError:
        return Response("Пустое значение", 400)


@app.route('/yandex/metrica/create_client', methods=['POST'])
def create_client():
    """Новая информация о клиентах добавляется(обновляется) к ранее загруженной."""
    try:
        json_file = request.get_json()
        token = json_file['token']
        if 'counter_id' in json_file:
            counter_id = json_file['counter_id']
        else:
            counter_id = get_counter(token)
        jsonfile = {'contacts': json_file['contacts']}
        create_client_info(token, counter_id, jsonfile)
        return Response("Клиенты добавлены/обновлены", 201)

    except BadRequestKeyError:
        return Response("Пустое значение", 400)


@app.route('/yandex/metrica/create_order', methods=['POST'])
def create_order():
    """Новая информация о заказах добавляется(обновляется) к ранее загруженной."""
    try:
        json_file = request.get_json(force=False)
        token = json_file['token']
        if 'counter_id' in json_file:
            counter_id = json_file['counter_id']
        else:
            counter_id=get_counter(token)
        jsonfile = {'orders': json_file['orders']}
        create_order_info(token, counter_id, jsonfile)
        return Response("Заказы добавлены/обновлены", 201)

    except BadRequestKeyError:
        return Response("Пустое значение", 400)


@app.route('/yandex/metrica/conversion_goal', methods=['POST'])
def conversion_goal():
    """Получить конверсии по целям"""
    try:
        json_file = request.get_json(force=False)
        token = json_file['token']
        if 'counter_id' in json_file:
            counter_id = json_file['counter_id']
        else:
            counter_id = get_counter(token)

        if 'goal_id' in json_file:
            goal_id = json_file['goal_id']
        else:
            goal_id_ = get_goal_id(token, counter_id)
            goal_id = goal_id_[0]

        report = get_conversion_goal(token, goal_id, counter_id)
        return report

    except BadRequestKeyError:
        return Response("Пустое значение", 400)


@app.route('/yandex/metrica/utm', methods=['POST'])
def get_utm_report():
    """Выгрузка отчета UTM метки по цели"""
    try:
        json_file = request.get_json(force=False)
        token = json_file['token']
        if 'counter_id' in json_file:
            counter_id = json_file['counter_id']
        else:
            counter_id = get_counter(token)

        if 'goal_id' in json_file:
            goal_id = json_file['goal_id']
        else:
            goal_id_ = get_goal_id(token, counter_id)
            goal_id = goal_id_[0]

        report = get_utm_stat(token, goal_id, counter_id)

        return report

    except BadRequestKeyError:
        return Response("Пустое значение", 400)


@app.route('/yandex/metrica/offline_conv', methods=['POST'])
def post_offline_conversion():
    """Загрузка офлайн-конверсии"""
    try:
        json_file=request.get_json(force=False)
        token=json_file['token']
        if 'counter_id' in json_file:
            counter_id = json_file['counter_id']
        else:
            counter_id = get_counter(token)

        if 'client_id_type' in json_file:
            client_id_type = json_file['client_id_type']
        else:
            client_id_type = "CLIENT_ID"

        filename = to_csv(json_file, counter_id, 'offline')

        file = open(filename, 'rb').read()

        r = post_offline_conv(token, counter_id, client_id_type, file)
        print(r.json())
        return Response("Offline конверсия загружена", 201)

    except BadRequestKeyError:
        return Response("Пустое значение", 400)


@app.route('/yandex/metrica/post_calls', methods=['POST'])
def post_call():
    """Загрузка звонков"""
    try:
        json_file = request.get_json(force=False)
        token = json_file['token']
        if 'counter_id' in json_file:
            counter_id = json_file['counter_id']
        else:
            counter_id = get_counter(token)

        if 'client_id_type' in json_file:
            client_id_type = json_file['client_id_type']
        else:
            client_id_type = "CLIENT_ID"

        filename = to_csv(json_file, counter_id, 'calls')

        file = open(filename, 'rb').read()

        p = post_calls(token, counter_id, client_id_type, file)
        print(p.json())
        return Response("Звонки загружены", 201)

    except BadRequestKeyError:
        return Response("Пустое значение", 400)


@app.route('/yandex/metrica/status_orders', methods=['POST'])
def status():
    """Сопоставление статусов заказов"""
    try:
        json_file = request.get_json(force=False)
        token = json_file['token']

        if 'counter_id' in json_file:
            counter_id = json_file['counter_id']
        else:
            counter_id = get_counter(token)

        jsonfile = {'order_statuses': json_file['order_statuses']}

        r = status_orders(token, counter_id, jsonfile)
        print(r.json())
        return Response("Клиенты добавлены/обновлены", 201)

    except BadRequestKeyError:
        return Response("Пустое значение", 400)
