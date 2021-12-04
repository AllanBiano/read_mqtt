import paho.mqtt.client as mqttClient
import time
import ssl

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")

        global Connected                                 #Use global variable
        Connected = True                                 #Signal connection

    else:
        print("Connection failed")


def on_message(client, userdata, message):
    print(message.payload.decode("utf-8"))              #print message using decode utf-8: special characters for the portuguese language
    path_file = 'C:/Users/user_name/file.csv'           #file name with message from mqtt
    with open(path_file, 'a') as f:
       f.write(message.payload.decode("utf-8") + ", \n")
    
    #alternative:
    # f = open(path_file,'a')                             #open the file to only add more lines, 'a' means append,
    # f.write(message.payload.decode("utf-8") + ", \n")   #write in the file messages received from the mqtt, '\n' to write one message per line
    # f.close()                                           #close the file
    


Connected = False                                       #Global variable for the state of the connection

# "1.1.1.1.1" and port 1234 must be replaced!
broker_address= "1.1.1.1.1"                             #Broker address
port = 1234                                             #Broker port
user = ''                                               #Connection username
password = ''                                           #Connection password

client = mqttClient.Client("Python")                    #create new instance
client.username_pw_set(user, password=password)         #set username and password
client.on_connect= on_connect                           #attach function to callback
client.on_message= on_message                           #attach function to callback

# path_cem is only an example, must be replaced!
path_cem = 'C:/Users/user_name/file.pem'
client.tls_set(path_cem, tls_version=ssl.PROTOCOL_TLSv1_2)
client.tls_insecure_set(True)

client.connect(broker_address, port=port)               #connect to broker

client.loop_start()                                     #start the loop

while Connected != True:                                #Wait for connection
    time.sleep(0.1)

client.subscribe("#")

try:
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("exiting")
    client.disconnect()
    client.loop_stop()