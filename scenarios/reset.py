def main():
    msg_file =  open("../data/message.txt","w")
    msg_file.write('reset')
    msg_file.close()

if __name__ == '__main__':
    main()