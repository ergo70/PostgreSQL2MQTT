import select
import datetime
import psycopg2
import paho.mqtt.client as mqtt
from contextlib import closing

_DB_USER = ''


def _on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    #client.will_set("database/logical", "{'status': 'offline'}", qos=2, retain=True)


client = mqtt.Client()
client.on_connect = _on_connect

client.connect("localhost", 1883, 60)

client.loop_start()

with closing(psycopg2.connect(database="logical", user=_DB_USER)) as conn:
    with conn, conn.cursor() as cur:
        cur.execute("LISTEN mqtt;")
        while True:
            conn.commit()
            if select.select([conn], [], [], 5) != ([], [], []):
                conn.poll()
                conn.commit()
                while conn.notifies:
                    notify = conn.notifies.pop()
                    print("Got NOTIFY: {}, {}, {}, {}".format(
                        datetime.datetime.now(), notify.pid, notify.channel, notify.payload))
                    client.publish("database/logical",
                                   notify.payload, qos=2, retain=False)
