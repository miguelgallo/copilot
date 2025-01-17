import socket

import struct
import binascii

from settings import db_url, db_org, db_token, db_bucket, so_IP, so_port

from influxdb_client import InfluxDBClient, Point, WritePrecision

import time
import datetime

print("Recording status and temperature: press CTRL+C to stop")

#Example: 7.8 : read status and targets
buf8 = [0] * 16

#Header (16 bytes)
buf8[0] = 83   #; //Default: S5 Protocol: ‘S’ ASCII char = 83 (dec)
buf8[1] = 53   #; //Default: S5 Protocol: ‘5’ ASCII char = 53 (dec)
buf8[2] = 16   #; //Default: length of header
buf8[3] = 1    #; //Default: OP code ID
buf8[4] = 3    #; //Default: OP code Length
buf8[5] = 5    #; //OP code: 5=READ, 3=WRITE
buf8[6] = 3    #; //Default: ORG field
buf8[7] = 8    #; //Default: ORG field Length
buf8[8] = 1    #; //Default: ORG field ID
buf8[9] = 62   #; //DB number, in this case: DB62
buf8[10] = 0   #; //Start address, high byte
buf8[11] = 146     #; //Start address, low byte
buf8[12] = 0   #; //Words length, high byte
buf8[13] = 10     #; //Words length, low byte
buf8[14] = 255 #; //Default: Empty field
buf8[15] = 2   #; //Default: Length empty field

#Example: 7.9 : read only T measure
buf9 = [0] * 16

#Header (16 bytes)
buf9[0] = 83   #; //Default: S5 Protocol: ‘S’ ASCII char = 83 (dec)
buf9[1] = 53   #; //Default: S5 Protocol: ‘5’ ASCII char = 53 (dec)
buf9[2] = 16   #; //Default: length of header
buf9[3] = 1    #; //Default: OP code ID
buf9[4] = 3    #; //Default: OP code Length
buf9[5] = 5    #; //OP code: 5=READ, 3=WRITE
buf9[6] = 3    #; //Default: ORG field
buf9[7] = 8    #; //Default: ORG field Length
buf9[8] = 1    #; //Default: ORG field ID
buf9[9] = 62   #; //DB number, in this case: DB62
buf9[10] = 0   #; //Start address, high byte
buf9[11] = 104   #; //Start address, low byte
buf9[12] = 0   #; //Words length, high byte
buf9[13] = 2  #; //Words length, low byte
buf9[14] = 255 #; //Default: Empty field
buf9[15] = 2   #; //Default: Length empty field

#Initialization of variables
previousT = 20.0
isActive = -1
isMonitoring = -1
finalTarget = 20.0
currentTarget = 0
slopeT = 0

#Setup Influxdb client
#db_client = InfluxDBClient(url=db_url, token=db_token, org=db_org, write_precision=WritePrecision.S)
db_client = InfluxDBClient(url=db_url, token=db_token, org=db_org)

#Connect to Climate Chamber
so_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#Loop to reconnect
reconnect = True

while reconnect:
    try:
        #Sleep 5 seconds
        time.sleep(5)

        #Connect
        so_client.connect((so_IP, so_port))
        print("Climate Chamber: connected!")
                
        while True:
            #Sleep 5 seconds
            time.sleep(5)
            
            #print("Executing code...")

            #--------------------------------------------------------------
            #Send query for T measurement
            result = so_client.send(bytearray(buf8))

            #Receive data
            #size = 16+(4+6)*2
            data_S = so_client.recv(36)

            #Convert data into variable
            isMonitoring = int.from_bytes(data_S[16:17], byteorder='big')
            isActive = int.from_bytes(data_S[17:18], byteorder='big')
            #bit_1 = isMonitoring
            #if bit_1 == 1:
            #    print("Temperature monitoring: ON")
            #elif bit_1 == 3:
            #    print("Temperature and Rel.Hum. monitoring: ON")
            #elif bit_1 == 2:
            #    print("Temperature monitoring: OFF (Rel.Hum. ON)")
            #else:
            #    print("Temperature monitoring: OFF")
            #bit_2 = isActive
            #if bit_2 == 1:
            #    print("Chamber: Active")
            #else:
            #    print("Chamber: OFF")
                    
                    
            for i in range(3):
                ini = 16+8+i*4
                fin = ini+4
                #print("Iteration: ",i,"@",ini,":",fin,"= register",ini-16-8+154)
                data_block = data_S[ini:fin]
                output = struct.unpack_from('>i', bytearray(data_block))
                ieee = hex(int(str(output)[1:-2]))
                flo = struct.unpack(">f", int(ieee,16).to_bytes(4, byteorder='big', signed=True))[0]
                if i == 0:
                    finalTarget = round(flo,2)
                    #print("Final target:",finalTarget,"°C")
                if i == 1:
                    currentTarget = round(flo,2)
                    #print("Current target:",currentTarget,"°C")
                if i == 2:
                    slopeT = round(flo,2)
                    #print("Slope:",slopeT,"°C/min")
        

            #--------------------------------------------------------------
            #Send query for T measurement
            result = so_client.send(bytearray(buf9))
            
            #Receive data
            #size = 16+2*2
            data_T = so_client.recv(20)
            
            #Convert data into variable
            data_block = data_T[16:20]
            output = struct.unpack_from('>i', bytearray(data_block))
            ieee = hex(int(str(output)[1:-2]))

            ieee = hex(int(str(output)[1:-2]))
            flo = struct.unpack(">f", int(ieee,16).to_bytes(4, byteorder='big', signed=True))[0]
            newT = round(flo,2)
            #print("Chamber T = ", newT)

            #Print the time (int for seconds)
            theTime = int(time.time())
            #print("Time is", theTime)
            date_time = datetime.datetime.fromtimestamp(theTime)
            #print("Converted time is:",date_time)
            
            #if newT is different from the previous, save it in influxdb
            if newT != previousT:
                previousT = round(flo,2)
                print("New T = ",newT," @ ",date_time)
                

                if isActive and isMonitoring:
                    if currentTarget != finalTarget :
                        #New point with T, target + slope and status
                        point = Point("temperature").field("chamber_T",newT).field("chamber_isActive",isActive).field("chamber_isMonitoring",isMonitoring).field("chamber_finalTarget",finalTarget).field("chamber_currentTarget",currentTarget).field("chamber_slopeT",slopeT)
                    else:
                        #New point with T, target and status
                        point = Point("temperature").field("chamber_T",newT).field("chamber_isActive",isActive).field("chamber_isMonitoring",isMonitoring).field("chamber_finalTarget",finalTarget)                
                    
                else:
                    #New point with T only
                    #point = Point("temperature").field("chamber_T",newT).time(theTime) #time is useless
                    point = Point("temperature").field("chamber_T",newT)
                        
                
                db_client.write_api().write(bucket=db_bucket, org=db_org, record=point)
                                
    except KeyboardInterrupt:
        print("TRL+C pressed. Closing client connection...")
        #Close communication
        so_client.close()
        db_client.close()
        print("Done. Client closed.")
        reconnect = False
        
    except Exception:
        print('Failed to connect. Retrying...')
