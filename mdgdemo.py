import datetime
import random
import threading
import sys
import time
import prometheus_client
from prometheus_client import Counter,Gauge


from flask import Flask, request

FLASK_PORT = 10001
print('hello gautam it is running')
class MDG_MQTT():

    def __init__(self):
        
        self.flaskapp = Flask(__name__)
        self.flaskPort = FLASK_PORT
        self.enable_debug = False
        self.starttime = 0
        self.payload_counter = 0
        self.num_of_devices = 2
        self.mdg_settings = {
            "pooling_freq" : 1000,
            "data_vol" : 2
        }
        self.temp_data = []
        self.humid_data = []
        self.ir_data = []
        self.moisture_data = []
        self.pressure_data = []
        self.devide_ID = []
        self.FLAG_ENABLE_DATA_GEN = False
        self.tempRange = [90.0,99.9]
        self.humidityRange = [52.0,95.0]
        self.moistureRange = [22.0,99.0]
        self.infraredRange = [0,2]
        self.pressureRange = [20.0,99.9]
        self.maxLionxInMDG = 8
        self.timeinterval = 1
        self.begin_time = 10
        self.mdfOffSet = 100
        

        args = sys.argv
        if len(args) > 1:
            self.flaskPort = args[1]
            self.MDG_Num = args[2]


        self.createDeviceID(int(self.MDG_Num))
        print("Device ID: ",self.devide_ID)


        self.graphs = {}
        self.graphs['data_vol'] = Counter('Num_Of_Total_Generated_Data', 'The total number of processed requests')
        self.graphs['requested_data_vol'] = Gauge('Requested_Data_Volume', 'The total number of unprocessed requests')
        
        thread1 = threading.Thread(target=self.flaskapp.run(host='0.0.0.0',port=self.flaskPort,debug=False))
        thread1.start()

        thread2 = threading.Thread(target=self.dataGenPooling)
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
                self.timeinterval = int(json['pooling_freq'])
                self.mdg_settings['data_vol'] = int(json['data_vol'])
                self.num_of_devices = int(json['num_of_devices'])
                self.payload_counter = 0
                self.graphs['requested_data_vol'].set(0)
                self.begin_time = time.time()

                for i in range(0,self.mdg_settings['data_vol']):
                    self.graphs['requested_data_vol'].inc()

            except:
                pass


    def createDeviceID(self,mdg_num):
        for i in range(self.maxLionxInMDG):
            self.devide_ID.append("MDG_"+ str(self.mdfOffSet + ((mdg_num - 1)*8+i+1)))


    def createRecords(self):
        try:
            self.temp_data = []
            self.humid_data = []
            self.ir_data = []
            self.moisture_data = []
            self.pressure_data = []
            for i in range(0,self.num_of_devices):
                datetime_object = datetime.datetime.now()
                data1= {'identity':{'device_id':str(self.devide_ID[i])},
                            'process_data':{'timestamp':str(datetime_object.strftime("%y-%m-%d %H:%M:%S")),
                                'temperature': str(round(random.uniform(self.tempRange[0],self.tempRange[1]),1))}}
                
                data2= {'identity':{'device_id': str(self.devide_ID[i])},
                            'process_data':{'timestamp': str(datetime_object.strftime("%y-%m-%d %H:%M:%S")),
                                'humidity': str(round(random.uniform(self.humidityRange[0],self.humidityRange[1]),1))}}

                data3= {'identity': {'device_id': str(self.devide_ID[i])},
                            'process_data': {'timestamp': str(datetime_object.strftime("%y-%m-%d %H:%M:%S")),
                                'infrared': str(round(random.uniform(self.infraredRange[0],self.infraredRange[1]),1))}}

                data4= {'identity': {'device_id': str(self.devide_ID[i])},
                            'process_data': {'timestamp': str(datetime_object.strftime("%y-%m-%d %H:%M:%S")),
                                'moisture': str(round(random.uniform(self.moistureRange[0],self.moistureRange[1]),1))}}

                if self.FLAG_ENABLE_DATA_GEN:
                    data5= {'identty': {'device_id': str(self.devide_ID[i])},
                                'process_data':{'timestamp': str(datetime_object.strftime("%y-%m-%d %H:%M:%S")),
                                    'pressure': str(round(random.uniform(self.pressureRange[0],self.pressureRange[1]),1))}}

                self.temp_data.append(data1)
                self.humid_data.append(data2)
                self.ir_data.append(data3)
                self.moisture_data.append(data4)
                if self.FLAG_ENABLE_DATA_GEN:
                    self.pressure_data.append(data5)

        except Exception as e:
            print("Exception in create records: ", e)


    def dataGenPooling(self):
        while(True):
            if(self.FLAG_ENABLE_DATA_GEN == True):
                if( (time.time() - self.begin_time) < int(self.mdg_settings['data_vol'])):
                    if( (time.time() - self.starttime) > (int(self.timeinterval) / 1000)):
                        print('***** Sent *****')
                        self.starttime = time.time()
                        self.payload_counter = self.payload_counter + 1
                        self.createRecords()


    
if __name__ == "__main__":
    MDG_MQTT()