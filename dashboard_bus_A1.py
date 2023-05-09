import csv
import logging
from logging.handlers import TimedRotatingFileHandler
from tb_gateway_mqtt import TBGatewayMqttClient
import time, threading
import random

log_level = logging.INFO
log_filename = './log/' + 'bus_A1.log'
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
idx = 0
bus_wating_time = 10
bus_speed = 0
bus_fuel_level = 80
bus_temp = 25
bus_engine_temp = 195
bus_occupancy = 50
bus_health_status = 'Healthy'
bus_health_des = 'Operational'
bus_health_color = '#7FB77E'
ac_faulty = False
engine_faulty = False
bus_occupancy_fixed = 0
bus_healthy_count = 5
bus_warning_count = 0
bus_damaged_count = 0

def setInterval(func,time):
    e = threading.Event()
    while not e.wait(time):
        func()


def publishTelemetry():
    global bus_fuel_level
    global bus_longitude, bus_latitude, bus_speed,  bus_stop,bus_stop_status, bus_fuel_level, bus_temp, bus_engine_temp, bus_occupancy, bus_eta, bus_wating_time, bus_health_status, bus_health_color,bus_health_des,bus_occupancy_fixed
    global ac_faulty,engine_faulty

    if publish == True:

        get_csv_data()

        execute_msg(read_msg())
        
        if engine_faulty:
            bus_down()
        
        usual_routine()

def execute_msg(msg):
    global bus_longitude, bus_latitude, bus_speed,  bus_stop,bus_stop_status, bus_fuel_level, bus_temp, bus_engine_temp, bus_occupancy, bus_eta, bus_wating_time, bus_health_status, bus_health_color,bus_health_des,bus_occupancy_fixed
    global ac_faulty,engine_faulty
    global bus_healthy_count, bus_warning_count, bus_damaged_count

    if msg == 'refuel':
        log.info("Refuel Message Received")
        bus_fuel_level = 100
        clear_msg()

    if 'deplete_fuel' in msg:
        log.info("Deplete Message Received")
        bus_fuel_level = float(msg.split(' ')[1])
        clear_msg()

    if 'reset' in msg:
        log.info("Reset Message Received")
        
        bus_speed = 0
        bus_fuel_level = 80
        bus_temp = 25
        bus_engine_temp = 195
        bus_occupancy = 50
        bus_health_status = 'Healthy'
        bus_health_des = 'Operational'
        bus_health_color = '#7FB77E'
        bus_healthy_count = 5
        bus_warning_count = 0
        bus_damaged_count = 0
        ac_faulty = False
        
        clear_msg()
        
    
    if 'ac_faulty' in msg:
        log.info("A/C Faulty Message Received")
        ac_faulty = True
        clear_msg()

    if 'ac_repair' in msg:
        log.info("A/C Repair Message Received")
        ac_faulty = False
        clear_msg()

    if 'fixed_bus_multi' in msg:
        log.info("Fix Occupany Multiplier Message Received")
        bus_occupancy_fixed = float(msg.split(' ')[1])
        clear_msg()
    
    if 'hot_engine' in msg:
        log.info("Hot Engine Message Received")
        engine_faulty = True
        clear_msg()


def read_msg():
    msg_file = open("./data/message.txt","r")
    msg = msg_file.readline() 
    msg_file.close()
    return msg

def update_bus_status_count(healthy_count, warning_count, damaged_count):
    global bus_healthy_count, bus_warning_count, bus_damaged_count
    bus_healthy_count = healthy_count 
    bus_warning_count = warning_count 
    bus_damaged_count = damaged_count


def update_health(health_status,health_des, health_color):
    global bus_health_status, bus_health_color,bus_health_des
    
    bus_health_status = health_status
    bus_health_des = health_des
    bus_health_color = health_color

def clear_msg():
    msg_file = open("./data/message.txt","w")
    msg_file.write('')
    msg_file.close()

def get_csv_data():
    global bus_longitude, bus_latitude, bus_speed,  bus_stop,bus_stop_status, bus_eta

    bus_longitude = list_CSV[idx][1]
    bus_latitude = list_CSV[idx][2]
    bus_stop = list_CSV[idx][3]
    bus_stop_status = list_CSV[idx][4]
    bus_eta = int(list_CSV[idx][5]) * 2
    bus_eta = str(int(bus_eta) // 60) + ' min ' + str(int(bus_eta)  % 60) + ' sec'

def bus_down():
    global bus_longitude, bus_latitude, bus_speed,  bus_stop,bus_stop_status, bus_fuel_level, bus_temp, bus_engine_temp, bus_occupancy, bus_eta, bus_wating_time, bus_health_status, bus_health_color,bus_health_des
    global bus_healthy_count, bus_warning_count, bus_damaged_count
    global engine_faulty

    msg = ''
    health_des = 'Engine Overheated'
    emergency_button = '<div id=\"btn_contact_tech\" style=\"border:solid;user-select:none;display:block; width: 90%; font-size:1rem;text-align:center;padding:5px; margin-top:12px;cursor:pointer;\">Contact Technician</div>'
    update_health('Damaged',health_des, '#E94560')
    update_bus_status_count(4,0,1)

    while msg != 'engine_repair':
        
        
        bus_engine_temp = get_engine_temp(engine_faulty)
        bus_eta = '- min - sec'

        send_telemetry(bus_longitude, 
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
                bus_health_color, bus_healthy_count, bus_warning_count, bus_damaged_count,emergency_button)

        time.sleep(1)

        bus_temp = round(random.uniform(32.0, 35.0),1)
        bus_speed = 0
        bus_occupancy = get_bus_occupancy(bus_occupancy,-1)

        msg = read_msg()
    

    update_health('Healthy','Operational', '#7FB77E')
    update_bus_status_count(5,0,0)
    engine_faulty = False
    bus_engine_temp = get_engine_temp(engine_faulty)
    clear_msg()



def usual_routine():
    global gateway, list_CSV, publish, idx
    global bus_longitude, bus_latitude, bus_speed,  bus_stop,bus_stop_status, bus_fuel_level, bus_temp, bus_engine_temp, bus_occupancy, bus_eta, bus_wating_time, bus_health_status, bus_health_color
    global bus_healthy_count, bus_warning_count, bus_damaged_count

    if int(list_CSV[idx][5]) == 0:
        wait_counter = 0
        bus_speed = 0
        bus_occupancy_multiplier = get_bus_occupancy_multiplier(bus_occupancy_fixed)
        while wait_counter < bus_wating_time:

            send_telemetry(bus_longitude, 
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
                bus_health_color, bus_healthy_count, bus_warning_count, bus_damaged_count,'')


            time.sleep(1)

            bus_occupancy = get_bus_occupancy(bus_occupancy ,bus_occupancy_multiplier)
            bus_fuel_level = consume_fuel(bus_fuel_level)
            bus_temp = get_ac_temp(ac_faulty)
            bus_engine_temp  = get_engine_temp(engine_faulty)
            set_health_status(ac_faulty, bus_fuel_level)

            wait_counter +=1

    else:

        bus_speed = get_speed(bus_speed, False)
        bus_fuel_level = consume_fuel(bus_fuel_level)
        bus_temp = get_ac_temp(ac_faulty)
        bus_engine_temp  = get_engine_temp(engine_faulty)
        set_health_status(ac_faulty, bus_fuel_level)

        send_telemetry(bus_longitude, 
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
                bus_health_color, bus_healthy_count, bus_warning_count, bus_damaged_count,'')
    
        
    idx+=1
    if (idx == len(list_CSV)):
        idx = 0

def set_health_status(_ac_faulty, _bus_fuel_level):
    health_des = ''
    health_status = ''
    health_color = ''
    warning_count = 0
    healthy_count = 5

    if _ac_faulty:
        health_des = "A/C Down"
        health_status = 'Warning'
        health_color = '#FFAE6D'
        warning_count = 1
        healthy_count = 4

    if _bus_fuel_level < 20.0 and _ac_faulty:
        health_des += ", Low Fuel"
        warning_count = 1
        healthy_count = 4
    elif _bus_fuel_level < 20.0 and not _ac_faulty:
        health_des = "Low Fuel"
        health_status = 'Warning'
        health_color = '#FFAE6D'
        warning_count = 1
        healthy_count = 4
    if not _ac_faulty and _bus_fuel_level >= 20.0 :
        health_des = 'Operational'
        health_status = "Healthy"
        health_color = '#7FB77E'
        warning_count = 0
        healthy_count = 5
    
    update_health(health_status,health_des,health_color)
    update_bus_status_count(healthy_count,warning_count,0)




def get_bus_occupancy(curr_occupancy ,multiplier):
    bus_occupancy = curr_occupancy +  round(random.random() * 5,1) *multiplier
    bus_occupancy = 0 if bus_occupancy < 0 else bus_occupancy
    bus_occupancy = 100 if bus_occupancy > 100 else bus_occupancy

    return bus_occupancy


def get_bus_occupancy_multiplier(fixed):
    if fixed == 0:
        if ac_faulty:
            return -1.5
        else:
            return 1 if random.random() > 0.5  else -1
    else:
        return fixed

def consume_fuel(curr_level):
    global bus_health_status, bus_health_color,bus_health_des
    if curr_level > 5:
        curr_level = round(curr_level - random.random() * 0.05,2)

    return curr_level

def get_ac_temp(faulty):
    if faulty:
        return round(random.uniform(35.0, 40.0),1)
    else:
        return round(random.uniform(22.5, 25.0),1)

def get_engine_temp(faulty):
    if not faulty:
        return round(random.uniform(195.0, 220.0),1)
    else:
        return round(random.uniform(290.0, 310.0),1)

def get_speed(bus_speed,stopping):
    if not stopping:
        if bus_speed == 0:
            return round(random.uniform(5.0, 15.0),1)
        else:
            return round(random.uniform(40.0, 60.0),1)


def send_telemetry(bus_longitude, 
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
                    bus_health_color, bus_healthy_count, bus_warning_count, bus_damaged_count,emergency_feature):

    
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
                                                                    "healthy_count":bus_healthy_count,
                                                                    "warning_count":bus_warning_count,
                                                                    "damaged_count":bus_damaged_count,
                                                                    "emergency_feature": emergency_feature}}

    # log.debug("{}) {} {}".format(idx, list_CSV[idx][0], telemetry)) 
    
    gateway.gw_send_telemetry(list_CSV[idx][0], telemetry)




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
  global gateway, list_CSV
  
  gateway = TBGatewayMqttClient(host="", token="")
  gateway.connect()
  gateway.gw_set_server_side_rpc_request_handler(rpc_request_response)
  
  gateway.gw_connect_device("Bus A1", "CampusBus")
  
#   time.sleep(5)
  
#   attribute_busA = {"color": "blue"}
#   gateway.gw_send_attributes("Bus A1", attribute_busA)
#   log.info("Publish Bus A1 Attribute - Color")
  
#   time.sleep(5)
  
  file_CSV = open("./data/bus_telemetry_A1.csv")
  data_CSV = csv.reader(file_CSV)
  list_CSV = list(data_CSV)
  log.info("Read Telemetry CSV. Total Record: %s" % len(list_CSV))
  
#   time.sleep(5)
  log.info("Bus Running")
  setInterval(publishTelemetry,1)

if __name__ == '__main__':
    main()