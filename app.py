import os
import shutil
from subprocess import Popen
import threading
from flask import Flask, request


class MDG_APP():
    def __init__(self) -> None:
        
        self.flaskApp = Flask(__name__)

        self.active_ports = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        self.number_of_mdg = 0

        thread1 = threading.Thread(target=lambda:self.flaskApp.run(host='0.0.0.0',port=20231,debug=True,use_reloader=False))
        thread1.start()

        @self.flaskApp.route('/createmdg',methods=['POST'])
        def createmdg():
            json = request.get_json()
            port = int(json['port'])
            if 0 < port <= 65535:
                if port not in self.active_ports:
                    self.create_MDG(port)
                    return "{\n\t'status':'success',\n\t'message':'success'\n}"
                else:
                    return "{\n\t'status':'failure',\n\t'message':'port already running'\n}"
            else:
                return "{\n\t'status':'invalid port',\n\t'message':'valid port range (0-65535)'\n}"
            

        @self.flaskApp.route("/runningmdg", methods=['GET'])
        def runningMDGCount():
            temp = []
            for i in range(0, len(self.active_ports)):
                if self.active_ports[i] != 0:
                    temp.append(str(self.active_ports[i]))
                #     print(self.active_ports[i])
                # else:
                #     print("this is empty index")
            return "{\n\t'status':'success',\n\t'message':"+ str(temp) + "\n}"
            # return "{\n\t\"status\":\"success\",\n\t\"message\":\""+ str(self.active_ports) + "\"\n}"


        @self.flaskApp.route("/deletemdg",methods=['POST'])
        def deleteMDG():
            json = request.get_json()
            port = int(json['port'])
            print('delete MDG')
            if port in self.active_ports :
                cmd = f"kill $(lsof -ti :{port})"
                # Popen(cmd,shell=True)
                print(cmd)
                os.popen(cmd)
                script_file = f"mdg_{self.active_ports.index(port)+1}.py"
                try:
                    os.remove(script_file)
                    self.active_ports[self.active_ports.index(port)] = 0
                    print(f"Deleted {script_file}")
                except OSError as e:
                    print(f"Error deleting {script_file}: {e}")
                return "{\n\t'status':'success',\n\t'message':'MDG deleted'\n}"

            else:
                return "{\n\t'status':'success',\n\t'message':'No active service on this port'\n}"


    def create_MDG(self,port):
        print('in MDG')
        self.number_of_mdg = self.number_of_mdg + 1
        self.active_ports[self.number_of_mdg - 1] = port
        
        shutil.copyfile("mdg.py","mdg_"+str(self.number_of_mdg)+".py")
        cmd = "python3 mdg_"+str(self.number_of_mdg)+".py"+" "+ str(port) +" "+str(self.number_of_mdg)
        Popen(cmd,shell=True)



if __name__ == '__main__':         
    MDG_APP()