import datetime
import logging
from logging.handlers import SysLogHandler
import random
import socket
import threading
import sys
import time
import prometheus_client
from prometheus_client import Counter,Gauge
from network.receiver import Mqtt_Receiver
from network.sender import Mqtt_Sender
import settings

from flask import Flask, request

FLASK_PORT = 10001
print('hello gautam it is running')
class MDG_MQTT():

    def __init__(self):
        
        self.flaskapp = Flask(__name__)
        self.flaskPort = FLASK_PORT
        self.enable_debug = False
        self.starttime = 0
        self.FLAG_ENABLE_PRESSURE = False 
        self.payload_counter = 0
        self.num_of_devices = 2
        self.device_state = [0,0,0,0,0,0,0,0]
        self.sensor_state = [[],[],[],[],[],[],[],[]]
        self.mdg_settings = {
            "settimeinterval": 1000,
            "time_duration": 2
        }
        self.temp_data = []
        self.humid_data = []
        self.ir_data = []
        self.moisture_data = []
        self.pressure_data = []
        self.devide_ID = []
        self.sensor_ID = [[],[],[],[],[],[],[],[]]
        self.FLAG_ENABLE_DATA_GEN = False
        self.tempRange = [[92.0,99.9],[92.0,99.9],[92.0,99.9],[92.0,99.9],[92.0,99.9],[92.0,99.9],[92.0,99.9],[92.0,99.9]]
        self.humidityRange = [[50.0,95.0],[50.0,95.0],[50.0,95.0],[50.0,95.0],[50.0,95.0],[50.0,95.0],[50.0,95.0],[50.0,95.0]]
        self.moistureRange = [[22.0,99.0],[22.0,99.0],[22.0,99.0],[22.0,99.0],[22.0,99.0],[22.0,99.0],[22.0,99.0],[22.0,99.0]]
        self.infraredRange = [[0,2],[0,2],[0,2],[0,2],[0,2],[0,2],[0,2],[0,2]]
        self.pressureRange = [[20.0,99.9],[20.0,99.9],[20.0,99.9],[20.0,99.9],[20.0,99.9],[20.0,99.9],[20.0,99.9],[20.0,99.9]]
        self.maxLionxInMDG = 8
        self.settimeinterval = 1
        self.sensorNum = 4
        self.begin_time = 10
        self.mdgOffSet = 1000
        self.dataflow_velocity_ref = 60
        

        args = sys.argv
        if len(args) > 1:
            self.flaskPort = args[1]
            self.MDG_Num = args[2]


        self.createDeviceID(int(self.MDG_Num))
        print("Device ID: ",self.devide_ID)


        self.graphs = {}
        self.graphs['time_duration'] = Counter('Num_Of_Total_Generated_Data', 'The total number of processed requests')
        self.graphs['requested_time_duration'] = Gauge('Requested_time_durationume', 'The total number of unprocessed requests')
        self.graphs['requested_timeinterval'] = Gauge('Requested_Time_Interval', 'The total number of unprocessed requests')
        self.graphs['dataflow_velocity'] = Gauge('dataflow_velocity', 'The total number of unprocessed requests')
        
        try:
            self.logger = self.get_logger(settings.SYSLOG_HOST,settings.SYSLOG_PORT)
        except Exception as e:
            print("Exception in logger init",e)

        self.init_mqtt()

        try:
            self.logger.info("MQTT_MDG HAS STARTED")
        except Exception as e:
            print("Exception in logger",e)

        
        thread1 = threading.Thread(target=self.dataGenPolling)
        thread1.start()

        thread2 = threading.Thread(target=lambda : self.flaskapp.run(host="0.0.0.0", port=self.flaskPort, debug=False))
        thread2.start()
        # self.dataGenPooling()

        @self.flaskapp.route('/printmdg',methods = ['GET'])
        def printname():
            return "hello gautam"

        @self.flaskapp.route('/generatedata',methods = ['POST'])
        def datagenerators():
            try:
                self.FLAG_ENABLE_DATA_GEN = True
                json = request.get_json()
                self.settimeinterval = int(json['settimeinterval'])
                self.graphs['requested_timeinterval'].set(int(self.settimeinterval))
                self.graphs['dataflow_velocity'].set((self.dataflow_velocity_ref * 1000) / int(self.settimeinterval))
                self.mdg_settings["time_duration"] = int(json["time_duration"])
                self.payload_counter = 0
                self.graphs['requested_time_duration'].set(0)
                self.begin_time = time.time()
                for i in range(0, self.mdg_settings["time_duration"]):
                    self.graphs['requested_time_duration'].inc()
                return "{\n\t\"status\":\"success\",\n\t\"message\":\"success\"\n}"
            except Exception as e:
                error_message = "Exception in generatedata: " + str(e)
                print(error_message)
                return "{\n\t\"status\":\"failure\",\n\t\"message\":\"" + error_message + "\"\n}"
        @self.flaskapp.route('/changedevicestate',methods = ['POST'])
        def changedevicestate():
            try:
                json = request.get_json()
                self.device_state[0] = int(json["lionx_1"]["state"])
                self.sensor_state[0][0] = int(json["lionx_1"]["temperature_state"])
                self.sensor_state[0][1] = int(json["lionx_1"]["humidity_state"])
                self.sensor_state[0][2] = int(json["lionx_1"]["infrared_state"])
                self.sensor_state[0][3] = int(json["lionx_1"]["moisture_state"])

                self.device_state[1] = int(json["lionx_2"]["state"])
                self.sensor_state[1][0] = int(json["lionx_2"]["temperature_state"])
                self.sensor_state[1][1] = int(json["lionx_2"]["humidity_state"])
                self.sensor_state[1][2] = int(json["lionx_2"]["infrared_state"])
                self.sensor_state[1][3] = int(json["lionx_2"]["moisture_state"])

                self.device_state[2] = int(json["lionx_3"]["state"])
                self.sensor_state[2][0] = int(json["lionx_3"]["temperature_state"])
                self.sensor_state[2][1] = int(json["lionx_3"]["humidity_state"])
                self.sensor_state[2][2] = int(json["lionx_3"]["infrared_state"])
                self.sensor_state[2][3] = int(json["lionx_3"]["moisture_state"])

                self.device_state[3] = int(json["lionx_4"]["state"])
                self.sensor_state[3][0] = int(json["lionx_4"]["temperature_state"])
                self.sensor_state[3][1] = int(json["lionx_4"]["humidity_state"])
                self.sensor_state[3][2] = int(json["lionx_4"]["infrared_state"])
                self.sensor_state[3][3] = int(json["lionx_4"]["moisture_state"])

                self.device_state[4] = int(json["lionx_5"]["state"])
                self.sensor_state[4][0] = int(json["lionx_5"]["temperature_state"])
                self.sensor_state[4][1] = int(json["lionx_5"]["humidity_state"])
                self.sensor_state[4][2] = int(json["lionx_5"]["infrared_state"])
                self.sensor_state[4][3] = int(json["lionx_5"]["moisture_state"])

                self.device_state[5] = int(json["lionx_6"]["state"])
                self.sensor_state[5][0] = int(json["lionx_6"]["temperature_state"])
                self.sensor_state[5][1] = int(json["lionx_6"]["humidity_state"])
                self.sensor_state[5][2]= int(json["lionx_6"]["infrared_state"])
                self.sensor_state[5][3] = int(json["lionx_6"]["moisture_state"])

                self.device_state[6] = int(json["lionx_7"]["state"])
                self.sensor_state[6][0] = int(json["lionx_7"]["temperature_state"])
                self.sensor_state[6][1] = int(json["lionx_7"]["humidity_state"])
                self.sensor_state[6][2] = int(json["lionx_7"]["infrared_state"])
                self.sensor_state[6][3] = int(json["lionx_7"]["moisture_state"])

                self.device_state[7] = int(json["lionx_8"]["state"])
                self.sensor_state[7][0] = int(json["lionx_8"]["temperature_state"])
                self.sensor_state[7][1] = int(json["lionx_8"]["humidity_state"])
                self.sensor_state[7][2] = int(json["lionx_8"]["infrared_state"])
                self.sensor_state[7][3] = int(json["lionx_8"]["moisture_state"])

                return "{\n\t\"status\":\"success\",\n\t\"message\":\"success\"\n}"
            
            except Exception as err:
                print("Exception in changedevicestate: ", err)
                return "{\n\t\"status\":\"success\",\n\t\"message\":\"invalid data received\"\n}"


    def createDeviceID(self,mdg_num):
        for i in range(self.maxLionxInMDG):
            self.devide_ID.append("MDG_"+ str(self.mdgOffSet + ((mdg_num - 1)*8+i+1)))
            for j in range(self.sensorNum):
                self.sensor_ID[i].append("Sid_"+str(self.mdgOffSet + (mdg_num - 1)*8+i+1) + "_" + str(j))
        print("***** Device and Sensor are configured *****")


    def get_logger(self,host:str,port:int)-> logging.Logger:
        logger = logging.getLogger("sample_application")
        logger.setLevel(logging.DEBUG)
        handler = SysLogHandler(address=(host,port),socktype=socket.SOCK_STREAM)
        handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s\n'))
        logger.addHandler(handler)
        return logger


    def init_mqtt(self):
        self.mdg_mqtt_receiver = Mqtt_Receiver(settings.BROKER_HOSTNAME, settings.BROKER_PORT,
                                               settings.MDG_CLIENT_ID+self.MDG_Num,
                                               settings.TOPIC_TO_RECEIVE_DATA)
        
        self.mdg_mqtt_receiver.mqtt_receiver_callback(self.receiver_callback_function)

        self.Mqtt_Sender = Mqtt_Sender(settings.BROKER_HOSTNAME,settings.BROKER_PORT,settings.SENDER_CLIENT_ID)

    def receiver_callback_function(self):
        pass


    def createRecords(self):
        try:
            self.temp_data = []
            self.humid_data = []
            self.ir_data = []
            self.moisture_data = []
            self.pressure_data = []
            for i in range(0,len(self.device_state)):
                print("In createRecords")
                if(self.device_state[i]):
                    print(f"Device no {i + 1} is enabel !!!")

                    datetime_object = datetime.datetime.now()
                    if(self.sensor_state[i][0]):
                        data1= {'identity':{'device_id':str(self.devide_ID[i])},
                                    'process_data':{'timestamp':str(datetime_object.strftime("%y-%m-%d %H:%M:%S")),
                                        'sensor_id':str(self.sensor_ID[i][0]),
                                        'temperature': str(round(random.uniform(self.tempRange[i][0],self.tempRange[i][1]),1))}}
                        self.temp_data.append(data1)

                    if(self.sensor_state[i][1]):
                        data2= {'identity':{'device_id': str(self.devide_ID[i])},
                                    'process_data':{'timestamp': str(datetime_object.strftime("%y-%m-%d %H:%M:%S")),
                                        'sensor_id':str(self.sensor_ID[i][1]),
                                        'humidity': str(round(random.uniform(self.humidityRange[i][0],self.humidityRange[i][1]),1))}}
                        self.humid_data.append(data2)

                    if(self.sensor_state[i][2]):
                        data3= {'identity': {'device_id': str(self.devide_ID[i])},
                                    'process_data': {'timestamp': str(datetime_object.strftime("%y-%m-%d %H:%M:%S")),
                                        'sensor_id':str(self.sensor_ID[i][2]),
                                        'infrared': str(round(random.uniform(self.infraredRange[i][0],self.infraredRange[i][1]),1))}}
                        self.ir_data.append(data3)
                    
                    if(self.sensor_state[i][3]):
                        data4= {'identity': {'device_id': str(self.devide_ID[i])},
                                    'process_data': {'timestamp': str(datetime_object.strftime("%y-%m-%d %H:%M:%S")),
                                        'sensor_id':str(self.sensor_ID[i][3]),
                                        'moisture': str(round(random.uniform(self.moistureRange[i][0],self.moistureRange[i][1]),1))}}
                        self.moisture_data.append(data4)

                    if(self.sensor_state[i][4]):
                        if self.FLAG_ENABLE_DATA_GEN:
                            data5= {'identity': {'device_id': str(self.devide_ID[i])},
                                        'process_data':{'timestamp': str(datetime_object.strftime("%y-%m-%d %H:%M:%S")),
                                            # 'sensor_id':str(self.sensor_ID[i][4]),
                                        'pressure': str(round(random.uniform(self.pressureRange[i][0],self.pressureRange[i][1]),1))}}
                            if self.FLAG_ENABLE_DATA_GEN:
                                self.pressure_data.append(data5)

                    # self.temp_data.append(data1)
                    # self.humid_data.append(data2)
                    # self.ir_data.append(data3)
                    # self.moisture_data.append(data4)

                    # if self.FLAG_ENABLE_DATA_GEN:
                    #     self.pressure_data.append(data5)

                    
                # print('*******************************************')
                # print("Temp Data:", self.temp_data)
                # print("Humid Data:", self.humid_data)

        except Exception as e:
            print("Exception in create records: ", e)


    def dataGenPolling(self):
        while(True):
            if(self.FLAG_ENABLE_DATA_GEN == True):
                if( (time.time() - self.begin_time) < (int(self.mdg_settings['time_duration']))):
                    if (time.time() - self.starttime) > (int(self.settimeinterval) / 1000):
                        print('***** Sent *****')
                        self.starttime = time.time()
                        self.payload_counter = self.payload_counter + 1
                        self.createRecords()
                        if self.temp_data:
                            self.Mqtt_Sender.mqtt_send(settings.TOPIC_TO_TRANSMIT_TEMP_DATA,self.temp_data)

                        if self.humid_data:
                            self.Mqtt_Sender.mqtt_send(settings.TOPIC_TO_TRANSMIT_HUMID_DATA,self.humid_data)

                        if self.ir_data:
                            self.Mqtt_Sender.mqtt_send(settings.TOPIC_TO_TRANSMIT_IR_DATA,self.ir_data)

                        if self.moisture_data:
                            self.Mqtt_Sender.mqtt_send(settings.TOPIC_TO_TRANSMIT_MOISTURE_DATA,self.moisture_data)

                        if self.FLAG_ENABLE_PRESSURE:
                            self.Mqtt_Sender.mqtt_send(settings.TOPIC_TO_TRANSMIT_PRESSURE_DATA,self.pressure_data)
                            
                        self.graphs['time_duration'].inc()
                        
                        try:
                            if self.FLAG_ENABLE_PRESSURE:
                                self.logger.info("PROCESS DATA- " + str(self.temp_data) + str(self.humid_data) + str(self.ir_data)
                                                + str(self.moisture_data) + str(self.pressure_data))
                            else:
                                self.logger.info("PROCESS DATA- " + str(self.temp_data) + str(self.humid_data) + str(self.ir_data)
                                                + str(self.moisture_data))
                        except Exception as e:
                            print("Exception in logger",e)
                        
                else:
                    self.FLAG_ENABLE_DATA_GEN = False
                    self.graphs['dataflow_velocity'].set(0)
                    print("Data Stopped")
            else:
                self.graphs['dataflow_velocity'].set(0)

                        

    
if __name__ == "__main__":
    MDG_MQTT()