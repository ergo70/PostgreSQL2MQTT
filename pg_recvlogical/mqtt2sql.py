import json
import psycopg2
import paho.mqtt.client as mqtt


def _parse_to_SQL(payload):
    print(payload)
    sql = ''
    message = json.loads(payload)
    if 'D' == message.get('action'):
        sql += 'DELETE FROM {}.{} WHERE '.format(
            message.get('schema'), message.get('table'))
        for key in message.get('identity'):
            sql += "{}='{}' AND ".format(key.get('name'),
                                         key.get('value'))
        sql = sql[:-5]
    elif 'B' == message.get('action'):
        sql += 'BEGIN'
    elif 'C' == message.get('action'):
        sql += 'COMMIT'
    elif 'T' == message.get('action'):
        sql += 'TRUNCATE TABLE {}.{}'.format(message.get('schema'),
                                             message.get('table'))
    elif 'I' == message.get('action'):
        sql += 'INSERT INTO {}.{} ('.format(message.get('schema'),
                                            message.get('table'))
        for column in message.get('columns'):
            sql += '{},'.format(column.get('name'))
        sql = sql[:-1]+') VALUES ('
        for column in message.get('columns'):
            sql += "'{}',".format(column.get('value'))
        sql = sql[:-1]+')'
    elif 'U' == message.get('action'):
        sql += 'UPDATE {}.{}'.format(message.get('schema'),
                                             message.get('table'))
        for column in message.get('columns'):
            sql += " SET {} = '{}',".format(column.get('name'),
                                           column.get('value'))
        sql = sql[:-1] + ' WHERE '.format(
            message.get('schema'), message.get('table'))
        for key in message.get('identity'):
            sql += "{}='{}' AND ".format(key.get('name'),
                                         key.get('value'))
        sql = sql[:-5]

    return sql+';'


def _on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("database/logical")


def _on_message(client, userdata, msg):
    # print(msg.topic + " " + str(msg.payload))
    print(_parse_to_SQL(msg.payload))


client = mqtt.Client()
client.on_connect = _on_connect
client.on_message = _on_message

client.connect("localhost", 1883, 60)

client.loop_forever()
