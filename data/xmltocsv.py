import xml.etree.ElementTree as et
import csv
import random


bus_stop_dic = {'103.76983+1.2942' : 'Kent Ridge Bus Terminal',
                '103.77055+1.2948' : 'LT 13',
                '103.7718+1.29345' : 'AS 5',
                '103.77386+1.29452' : 'COM 2',
                '103.77506+1.29305' : 'Biz 2',
                '103.77691+1.29372' : 'Opp TCOMS',
                '103.78036+1.29179' : 'Prince George\'s Park',
                '103.78469+1.29457' : 'Kent Ridge MRT',
                '103.781+1.29747' : 'LT 27',
                '103.7789+1.29718' : 'University Hall',
                '103.77616+1.29886' : 'Opp University Health Centre',
                '103.77421+1.29879' : 'Yusof Ishak House',
                '103.77284+1.29715' : 'Central Library'}

bus_stop_total = len(bus_stop_dic.keys())
bus_stop_index = 0
bus_stop_coords =  list(bus_stop_dic.keys())
bus_stop_names = list(bus_stop_dic.values())

def main():

    global status, nextstop, eta, bus_stop_index, bus_stop_total, bus_stop_coords, bus_stop_names

    route_kml = et.parse('A1Route.kml')
    coords_text = route_kml.getroot().text
    header_csv = ['Bus','Longitude','Latitude','Status','Next Stop','ETA']
    
    csv_file_name = 'bus_telemetry.csv'
    csv_file = open(csv_file_name, 'w+',newline='')
    csv_writer = csv.writer(csv_file)
    lines=0

    for coord in coords_text.splitlines():
        if coord == '':
            continue
        
        coord_list = list(map(float,coord.split(',')[0:2]))
        bus_coord= str(coord_list[0]) + '+' + str(coord_list[1])

        if bus_coord in bus_stop_coords:
            status = "Reached: " + bus_stop_names[bus_stop_index]

            if bus_stop_index + 1 >= bus_stop_total:
                bus_stop_index = 0
            else:
                bus_stop_index += 1

            nextstop = bus_stop_names[bus_stop_index]
            eta = 0
        else:
            status = "On Route"
            eta += 1

        bus_data = ['Bus A1'] + coord_list + [status, nextstop, eta]


        csv_writer.writerow(bus_data)
        lines+=1
        

    csv_file.close()
    print(str(lines) + ' of lines written into ' + csv_file_name)



if __name__ == '__main__':
    main()