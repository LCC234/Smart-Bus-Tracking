import xml.etree.ElementTree as et
import csv
import random

bus = 'K'

def main():

    global bus

    route_kml = et.parse( bus + 'Route.kml')
    coords_text = route_kml.getroot().text
    header_csv = ['Bus','Longitude','Latitude','Status','Next Stop','ETA']

    stop_list = ['Prince Geroge\'s Park','Kent Ridge MRT', 'LT 27', 'University Hall', 'Opp University Hall', 'Museum', 'University Town Hall', 'Opp University Hall', 'S 17', 'Opp Kent Ridge MRT', 'Engin 5', 'Takashimaya', 'Buffet Place', 'Techno', 'Sci 2', 'Casino', 'Zoo']
    
    csv_file_name = 'bus_telemetry_'+ bus +'.csv'
    csv_file = open(csv_file_name, 'w+',newline='')
    csv_writer = csv.writer(csv_file)
    lines=0
    stop = 'Prince Geroge\'s Park'
    eta = 1
    stop_counter = 0

    for coord in coords_text.splitlines():
        if  coord.strip() == '':
            continue
        
        data_list  = coord.split(',')
        coord_list = list(map(float,data_list[0:2]))
        # third_col = str(data_list[2])
        # bus_coord= str(coord_list[0]) + '+' + str(coord_list[1])

        # if stop == third_col:
        #     status = "Arrived"
        #     eta += 1
        # elif third_col != '0':
        #     stop = third_col
        #     status = "Next Stop"
        #     eta = 0
        # else:
        #     eta += 1
        eta -=1
        if eta == 0:
            status = 'Arrived'

            bus_data = ['Bus '+ bus] + coord_list + [stop ,status, eta]

            stop_counter += 1
            if stop_counter == len(stop_list):
                stop_counter=0

            stop = stop_list[stop_counter]
            status = 'Next Stop'
            eta = 45
        else:
            
            bus_data = ['Bus '+ bus] + coord_list + [stop ,status, eta]
            

        csv_writer.writerow(bus_data)
        lines+=1
        

    csv_file.close()
    print(str(lines) + ' of lines written into ' + csv_file_name)



if __name__ == '__main__':
    main()