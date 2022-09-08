import os
import csv

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

from tb_gateway_mqtt import TBGatewayMqttClient
import time, threading

idx = 0
publish = True

def setInterval(func,time):
    e = threading.Event()
    while not e.wait(time):
        func()
        
def publishTelemetry():
    global gateway, list_CSV, idx, publish
    
    if publish == True:
        for i in range (idx, idx + 4):
          telemetry = {"ts": int(round(time.time() * 1000)), "values": {"status": list_CSV[i][1], "fuel": list_CSV[i][2], "speed": list_CSV[i][3], "latitude": list_CSV[i][4], "longitude": list_CSV[i][5]}}
          log.debug("{}) {} {}".format(i, list_CSV[i][0], telemetry))
          gateway.gw_send_telemetry(list_CSV[i][0], telemetry)
          
        idx+=4;
        if (idx == len(list_CSV)):
            idx = 0;
    
    
def rpc_request_response(request_id, request_body):
    global publish
    
    print(request_body)
    method = request_body["data"]["method"]
    device = request_body["device"]
    req_id = request_body["data"]["id"]    
    if method == 'setPublish':
        params = request_body["data"]["params"]
        if params == True:
            publish = False;
        else:
            publish = True;
            
        gateway.gw_send_rpc_reply(device, req_id, params)  
          
    elif method == 'getPublish':  
        gateway.gw_send_rpc_reply(device, req_id, "False" if publish==True else "True")
        print("False" if publish==True else "True")
      
        
def main():
  global gateway, list_CSV, t1
  
  gateway = TBGatewayMqttClient(host=os.environ.get('TB_MQTT_HOST', "things.hpe-innovation.center"), token=os.environ.get('TB_MQTT_ACCESSTOKEN', "smartbus"), port=int(os.environ.get('TB_MQTT_PORT', 8884)))
  gateway.connect(tls=os.environ.get('TB_MQTT_TLS', True), ca_certs=os.environ.get('TB_MQTT_CACERT', "./vol/mqttserver.pub.pem"))
  gateway.gw_set_server_side_rpc_request_handler(rpc_request_response)
  
  gateway.gw_connect_device("Bus A", "bus")
  gateway.gw_connect_device("Bus B", "bus")
  gateway.gw_connect_device("Bus C", "bus")
  gateway.gw_connect_device("Bus D", "bus")
  
  time.sleep(5)
  
  attribute_busA = {"perimeter": "[[[37.77154424543907,-122.51144374652259],[37.76361023664128,-122.51083358838831],[37.764662971898176,-122.48599101314537],[37.7658975511296,-122.45876312255861],[37.76549051086049,-122.45769027632102],[37.765987359486004,-122.45230980146019],[37.775359270739486,-122.45436928956322],[37.77414064001368,-122.46211053809337],[37.773462335918424,-122.4700498487512],[37.772716181039634,-122.48618602752687],[37.77234362823952,-122.49472617168819],[37.77194322501647,-122.50234767406747]],[[37.77163056070347,-122.4946403596839],[37.773012184620605,-122.46491976596623],[37.774268513615944,-122.45505138547246],[37.76679813542369,-122.45354967539731],[37.76676417743611,-122.45826933563345],[37.76459083380007,-122.5095420081988],[37.770906936825654,-122.51009978622672]]]"}
  gateway.gw_send_attributes("Bus A", attribute_busA)
  attribute_busB = {"perimeter": "[[[37.782720965968664,-122.44820872247719],[37.77160880264001,-122.44593195778847],[37.77498879158632,-122.42043958002245],[37.78593608188649,-122.42273551123532]],[[37.78192584036643,-122.44713523630632],[37.78478765920679,-122.42451913482653],[37.784062136994606,-122.42340647481909],[37.775540182961805,-122.42156151672678],[37.77234669411838,-122.44548641564838]]]"}
  gateway.gw_send_attributes("Bus B", attribute_busB)
  log.info("Publish Bus A & B Attribute - Perimeter")
  
  time.sleep(5)
  
  file_CSV = open(os.environ.get('BUS_DATA_CSV', "./vol/bus_telemetry.csv"))
  data_CSV = csv.reader(file_CSV)
  list_CSV = list(data_CSV)
  log.info("Read Telemetry CSV. Total Record: %s" % len(list_CSV))
  
  time.sleep(5)
  setInterval(publishTelemetry,1)

if __name__ == '__main__':
    main()
