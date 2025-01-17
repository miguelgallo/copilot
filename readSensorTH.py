#import serial

import time
import datetime

import math

from urllib.request import urlopen

#from settings import db_url, db_org, db_token, db_bucket, so_IP, so_port
from settings import db_url, db_org, db_token, so_IP, so_port

from influxdb_client import InfluxDBClient, Point, WritePrecision

print("Recording temperature and humidity: press CTRL+C to stop")

#Configure serial port (Windows = COM7, Linux = /dev/ttyACM0)
#ser = serial.Serial('/dev/ttyACM0', 9600)
#ser = serial.Serial('COM5', 9600)

#Setup Influxdb client
db_client = InfluxDBClient(url=db_url, token=db_token, org=db_org)

try:
   #Sleep 5 seconds
   time.sleep(5)

   while True:
      #if ser.in_waiting > 0:
      #   line = ser.readline().decode().rstrip()
         #print(line)

      #   tem, hum = line.split()
      
      time.sleep(5)

      previousT = 20.0
      db_bucket = []
      temp_url = []
      humi_url = []
      dew_url = []
      T = []
      RH = []
      DewP = []
      point = []
   
      for i in range(5):
         db_bucket.append("THSensor{}".format(i+1))

         temp_url.append(urlopen("http://193.206.149.13:22001/temperature{}".format(i+1)).read())
         humi_url.append(urlopen("http://193.206.149.13:22001/humidity{}".format(i+1)).read())
         dew_url.append(urlopen("http://193.206.149.13:22001/dewpoint{}".format(i+1)).read())

         T.append(float(temp_url[i].decode('utf-8')))
         RH.append(float(humi_url[i].decode('utf-8')))
         DewP.append(float(dew_url[i].decode('utf-8')))

         #newT = float(T)
         #newRH = float(RH)
         #newDewP = float(DewP)

         #Print the time (int for seconds)
         theTime = int(time.time())
         #print("Time is", theTime)
         date_time = datetime.datetime.fromtimestamp(theTime)
         #print("Converted time is:",date_time)
          
         #Avoid random values (residual communication by Arduino)
         #if newT > 125 or newT < -50:
         #   newT = previousT
          
         #if T[i] != previousT:
         #   previousT = T[i]
         print("T{} = ".format(i+1),T[i],"°C,  RH{} = ".format(i+1),RH[i],"%, Dew{} = ".format(i+1),DewP[i],"°C @ ",date_time)
         
         point.append(Point("temperature").field("sensor_T",T[i]).field("sensor_RH",RH[i]).field("sensor_DP",DewP[i]))
         db_client.write_api().write(bucket=db_bucket[i], org=db_org, record=point[i])

except KeyboardInterrupt:
  print("TRL+C pressed. Closing connection...")
  db_client.close()
  print("Done.")

except Exception:
  print('Failed for some reason...')
