import csv
import logging
from logging.handlers import TimedRotatingFileHandler
from tb_gateway_mqtt import TBGatewayMqttClient
import time, threading
import random

log_level = logging.INFO
log_filename = './log/' + 'others.log'
log = logging.getLogger(__name__)
log.setLevel(log_level)

## Here we define our formatter
formatter = logging.Formatter('%(asctime)s, %(name)s %(levelname)s %(message)s','%Y-%m-%d %H:%M:%S')

logHandler = TimedRotatingFileHandler(log_filename,  when="midnight", interval=1)
logHandler.setLevel(log_level)
## Here we set our logHandler's formatter
logHandler.setFormatter(formatter)

log.addHandler(logHandler)

publish = True
idx_d1 = 0
occupancy_d1 = 50
stop_counter_d1 = 0

idx_d2 = 0
occupancy_d2 = 50
stop_counter_d2 = 0

idx_e = 18
occupancy_e = 40
stop_counter_e = 0

idx_k = 36
occupancy_k = 20
stop_counter_k = 0



def setInterval(func,time):
    e = threading.Event()
    while not e.wait(time):
        func()

def publishTelemetry():
    global gateway, publish
    global list_CSV_d1, idx_d1, occupancy_d1,stop_counter_d1
    global list_CSV_d2, idx_d2, occupancy_d2,stop_counter_d2
    global list_CSV_e, occupancy_e,idx_e,stop_counter_e
    global list_CSV_k, occupancy_k,idx_k,stop_counter_k 
    
    if publish == True:

        #D1
        occupancy_d1,idx_d1,stop_counter_d1 = compute_bus_telemetry("Bus D1", occupancy_d1, list_CSV_d1,idx_d1,stop_counter_d1)

        #D2
        occupancy_d2,idx_d2,stop_counter_d2 = compute_bus_telemetry("Bus D2", occupancy_d2, list_CSV_d2,idx_d2,stop_counter_d2)

        #E
        occupancy_e,idx_e,stop_counter_e = compute_bus_telemetry("Bus E", occupancy_e, list_CSV_e,idx_e,stop_counter_e)

        #K
        occupancy_k,idx_k,stop_counter_k = compute_bus_telemetry("Bus K", occupancy_k, list_CSV_k,idx_k,stop_counter_k)


def compute_bus_telemetry(bus_name,occupancy, telemetry_list, idx, stop_counter):
    occupancy = get_occupancy(occupancy, telemetry_list[idx][4])
    eta = int(telemetry_list[idx][5]) * 2
    eta = str(int(eta) // 60) + ' min ' + str(int(eta)  % 60) + ' sec'
    send_telemetry(bus_name,
                    telemetry_list[idx][1],
                    telemetry_list[idx][2], 
                    get_speed(telemetry_list[idx][4]),
                    telemetry_list[idx][3],
                    telemetry_list[idx][4],
                    "Operational",
                    "80",
                    round(random.uniform(22.5, 25.5),1),
                    round(random.uniform(190.0, 210.0),1),
                    occupancy,
                    eta,
                    "Healthy",
                    '#7FB77E'
                    ,idx
                    )

    if telemetry_list[idx][4] == 'Arrived' and stop_counter < 10:
        stop_counter += 1
    elif telemetry_list[idx][4] == 'Arrived' and stop_counter == 10:
        idx+=1
        stop_counter = 0
    else:
        idx+=1
    
    if (idx == len(telemetry_list)):
        idx = 0

    return occupancy, idx, stop_counter
        

def get_occupancy(curr_occupancy, status):
    if status != "Arrived":
        return curr_occupancy
    else:
        multiplier = 1 if random.random() > 0.5  else -1
        bus_occupancy = curr_occupancy +  round(random.random() * 5,1) * multiplier
        bus_occupancy = 0 if bus_occupancy < 0 else bus_occupancy
        bus_occupancy = 100 if bus_occupancy > 100 else bus_occupancy
        return bus_occupancy



def get_speed(status):
    if status == "Arrived":
        return 0
    else:
        return round(random.uniform(50, 60),1)




def send_telemetry( bus_name,
                    bus_longitude, 
                    bus_latitude, 
                    bus_speed,  
                    bus_stop,
                    bus_stop_status, 
                    bus_health_des, 
                    bus_fuel_level, 
                    bus_temp, 
                    bus_engine_temp, 
                    bus_occupancy, 
                    bus_eta, 
                    bus_health_status,
                    bus_health_color,
                    idx):

    
    telemetry = {"ts": int(round(time.time() * 1000)), "values": {  "fuel": bus_fuel_level, 
                                                                    "speed": bus_speed, 
                                                                    "latitude": bus_latitude, 
                                                                    "longitude": bus_longitude, 
                                                                    "occupancy":bus_occupancy,
                                                                    "temp":bus_temp,
                                                                    "engine_temp": bus_engine_temp, 
                                                                    "eta":bus_eta,
                                                                    "health_status":bus_health_status,
                                                                    "health_des":bus_health_des,
                                                                    "bus_stop":bus_stop,
                                                                    "bus_stop_status":bus_stop_status,
                                                                    "bus_health_color":bus_health_color,
                                                                    "emergency_feature": ''}}

    # log.debug("{}) {} {}".format(idx, bus_name, telemetry)) 
    
    gateway.gw_send_telemetry(bus_name, telemetry)

def rpc_request_response(request_id, request_body):
    global publish
    
    print(request_body)
    method = request_body["data"]["method"]
    device = request_body["device"]
    req_id = request_body["data"]["id"]    
    if method == 'setPublish':
        params = request_body["data"]["params"]
        if params == True:
            publish = False
        else:
            publish = True
            
        gateway.gw_send_rpc_reply(device, req_id, params)  
          
    elif method == 'getPublish':  
        gateway.gw_send_rpc_reply(device, req_id, "False" if publish==True else "True")
        print("False" if publish==True else "True")
      

def main():
  global gateway, list_CSV_d1, list_CSV_d2,list_CSV_e,list_CSV_k
  
  gateway = TBGatewayMqttClient(host="", token="")
  gateway.connect()
  gateway.gw_set_server_side_rpc_request_handler(rpc_request_response)
  
  gateway.gw_connect_device("Bus D1", "CampusBus")
  gateway.gw_connect_device("Bus D2", "CampusBus")
  gateway.gw_connect_device("Bus E", "CampusBus")
  gateway.gw_connect_device("Bus K", "CampusBus")
  
#   time.sleep(5)
  
  attribute_busA = {"color": "blue"}
  gateway.gw_send_attributes("Bus A1", attribute_busA)
  log.info("Publish Bus D1 Attribute - Color")
  
#   time.sleep(5)
  
  file_CSV = open("./data/bus_telemetry_D1.csv")
  data_CSV = csv.reader(file_CSV)
  list_CSV_d1 = list(data_CSV)
  log.info("Read Telemetry CSV D1. Total Record: %s" % len(list_CSV_d1))

  file_CSV = open("./data/bus_telemetry_D2.csv")
  data_CSV = csv.reader(file_CSV)
  list_CSV_d2 = list(data_CSV)
  log.info("Read Telemetry CSV D2. Total Record: %s" % len(list_CSV_d2))

  file_CSV = open("./data/bus_telemetry_E.csv")
  data_CSV = csv.reader(file_CSV)
  list_CSV_e = list(data_CSV)
  log.info("Read Telemetry CSV E. Total Record: %s" % len(list_CSV_e))

  file_CSV = open("./data/bus_telemetry_K.csv")
  data_CSV = csv.reader(file_CSV)
  list_CSV_k = list(data_CSV)
  log.info("Read Telemetry CSV K. Total Record: %s" % len(list_CSV_k))
  
#   time.sleep(5)
  log.info("Buses Running")
  setInterval(publishTelemetry,1)

if __name__ == '__main__':
    main()