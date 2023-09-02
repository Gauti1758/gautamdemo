import paho.mqtt.client as receiver
import sys


class Mqtt_Receiver():
    def __init__(self,hostname,port,client_id,subscriber_topic):

        try:
            self.hostname = hostname
            self.port = port
            self.client_id = client_id
            self.subscriber_topic = subscriber_topic

            self.update_received_message = None
            self.enable_debug = False
            self.receiver_client_connected = False

            self.client = receiver.Client(client_id = self.client_id,clean_session=False)

            self.client.on_connect = self.receiver_on_connect
            self.client.on_message = self.receiver_on_message

            self.client.connect(host = self.hostname, port = self.port, keepalive = 10)
            self.client.loop_start()

        except Exception as ex:
                print("Exception in Receiver: "+ str(ex))
                sys.exit()

    
    def mqtt_receiver_callback(self, callback):
        self.update_received_message = callback

    
    def receiver_on_connect(self,client, userdata, flags, rc):
        if self.enable_debug:
            print("Connected with result code " + str(rc))
        if rc == 0 and self.enable_debug:
            print("***** Receiver is connected successfully with MQTT broker *****")
        elif rc != 0:
            print("***** Receiver is not connected with MQTT broker *****")

        self.client.subscribe(self.subscriber_topic)

    
    def receiver_on_message(self,client, userdata, message):
        if self.enable_debug:
            print("***** Received MSG from MQTT: "+message.topic + " " + str(message.payload) + " *****")
        self.update_received_message(str(message.payload.decode('utf-8')))


    def receiver_reconnect(self):
        if not self.receiver_client_connected:
            self.client.connect(host=self.hostname, port=self.port, keepalive=60)