from paho.mqtt import client as mqtt_client
import random
import time
import json
from POS.service.db import product_db
import secrets
from POS import views
import asyncio
import threading

# if __name__ == '__main__':
#     client = connect_mqtt()
#     run(client)
#     t_end = time.time() + 5
#     while time.time() < t_end:
#         pass

class MqttService:
    __broker = 'localhost'
    __port = 1883
    __topic = "/rfid/reader/scanned_tags"
    __client_id = lambda self:f'python-mqtt-{random.randint(0, 1000)}'
    __client: mqtt_client = None
    __last_scanned_tags = 0
    __scanned_tags = []

    def __init__(self):
            print("in init")

    def __new_tags_found(self):
        scanned_tags_copy = self.__scanned_tags.copy()
        if self.__last_scanned_tags != len(scanned_tags_copy):
            self.__last_scanned_tags = len(scanned_tags_copy)
            print("new tags fun", self.__last_scanned_tags, scanned_tags_copy)
            return True    
        return False


    async def tag_event_generator(self, request):
        while True:
            print("tag_event_generator")
            if await request.is_disconnected():
                print("stoped")
                self.stop()
                break

            if self.__new_tags_found():
                yield {
                    "event": "new_tags_found",
                    "id": secrets.token_hex(8),
                    "retry": 1000,
                    "data": self.__scanned_tags
                    }
            await asyncio.sleep(0.5)

    def update_scanned_tags(self, scanned_tag: dict):
        print(scanned_tag)
        if not scanned_tag.get('epc', None):
            return
        exisiting_tag = next((tag for tag in self.__scanned_tags if tag['epc'] == scanned_tag['epc']), None)
        if exisiting_tag:
            exisiting_tag['count'] += 1
        else:
            scanned_tag['count'] = 1
            print("updating tag", scanned_tag)
            self.__scanned_tags.append(scanned_tag)
        print(scanned_tag)

    def __disconnectt_mqtt(self):
        def on_disconnect(client, userdata, rc):
            self.__client.disconnect()
    #         print("disconnecting mqtt client", rc)
    #         self.__client.connected_flag=False
    #         self.__client.disconnect_flag=True
        

    def __connect_mqtt(self):
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)
        # Set Connecting Client ID
        client = mqtt_client.Client(self.__client_id())
        # client.username_pw_set(username, password)
        client.on_connect = on_connect
        client.connect(self.__broker, self.__port)
        self.__client = client
        print("mqtt connnected cleint is:", client)

    def __disconnect_mqtt(self):
        def stop(client):
            client = mqtt_client.Client(self.__client_id())
            client.disconnect()
            client.loop_stop()
            

    def __subscribe(self):
        def on_message(client, userdata, msg):
            print("Started subscribe fun")
            #print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
            tag = json.loads(msg.payload.decode())
            #print(tag)
            if tag.get('tagInventoryEvent', None):
                #print("s1_update",tag.get('tagInventoryEvent'))
                self.update_scanned_tags(tag.get('tagInventoryEvent'))

        self.__client.subscribe(self.__topic)
        self.__client.on_message = on_message
    
    def __closeMqtt(self):
        t_end = time.time() + 5
        while time.time() < t_end:
            pass
        print("Closing MQTT Client connection since 5 seconds passed")
        self.stop()
        #product_db.add_tags_to_product(self.scanned_tags,123)
        #products = product_db.get_product_details(self.scanned_tags)
        #print(self.scanned_tags, "are the scanned tags from the reader")
        if len(self.__scanned_tags) > 0:
            product_db.add_recent_scanned_tags(self.__scanned_tags)
            #print("Total scanned tags", self.__scanned_tags)
            self.__scanned_tags = []
            #self.__disconnect_mqtt(self)
            self.__client.disconnect()
        return True

    def start(self):
        print("Started MQTT")
        self.__connect_mqtt()
        if self.__client is None:
            print('client is None')
            return
        self.__subscribe()
        self.__client.loop_start()
        t = threading.Thread(target=self.__closeMqtt)
        t.daemon = True
        t.start()
        return True

    def stop(self):
        self.__client.loop_stop()
        return True	