import os
import subprocess
import paho.mqtt.client as mqtt

_DB_USER = ''


def _on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    #client.will_set("database/logical", "{'status': 'offline'}", qos=2, retain=True)


pg_recvlogical = subprocess.Popen(
    ['pg_recvlogical.exe', '-n', '-U', _DB_USER, '-d', 'logical', '--slot', 'mqtt_slot', '--start', '-o', 'format-version=2', '-f', '-'], stdout=subprocess.PIPE, universal_newlines=True)

client = mqtt.Client()
client.on_connect = _on_connect

client.connect("localhost", 1883, 60)

client.loop_start()

while True:
    payload = pg_recvlogical.stdout.readline().rstrip(os.linesep)

    if payload:
        print(payload)
        client.publish("database/logical", payload, qos=2, retain=False)
