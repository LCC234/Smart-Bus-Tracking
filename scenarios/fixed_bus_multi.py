import sys

# to unfix pass in no arguments

def main(fixed):
    msg_file =  open("../data/message.txt","w")
    msg_file.write('fixed_bus_multi '+ str(fixed))
    msg_file.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        main(0)
    else:
        main(sys.argv[1])