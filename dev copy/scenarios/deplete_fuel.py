import sys

def main(fuel_level):
    msg_file =  open("../data/message.txt","w")
    msg_file.write('deplete_fuel '+ str(fuel_level))
    msg_file.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        main(10)
    else:
        main(sys.argv[1])

