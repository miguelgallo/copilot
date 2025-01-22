import socket
import struct
import logging
import time
import math
from os import getcwd
from datetime import datetime
from urllib.request import urlopen
from influxdb_client import InfluxDBClient, Point

# Import settings
from settings import db_url, db_org, db_token, so_IP, so_port

# Logger configuration
DEBUG = False
LOG_FILE = True
FILE_NAME_FORMAT = "%Y-%m-%d_%H-%M-%S_climatic_chamber.log"

logger = logging.getLogger("ClimaticChamberAdapter")
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)

formatter = logging.Formatter('%(message)s')

if LOG_FILE:
    log_file = ""
    now = datetime.now()
    filename = now.strftime(FILE_NAME_FORMAT)
    log_file = getcwd() + '/logs/' + filename
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG if DEBUG else logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG if DEBUG else logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def float_to_hex(f):
    return hex(struct.unpack('<I', struct.pack('<f', f))[0])

class ClimaticChamber:
    def __init__(self, ip, read_port, write_port):
        self.ip = ip
        self.read_port = read_port
        self.write_port = write_port
        self.read_sock = None
        self.write_sock = None

    def connect(self):
        """Connects to the read and write ports of the climatic chamber."""
        try:
            self.read_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.read_sock.connect((self.ip, self.read_port))
            logger.info("Read connection established with the climatic chamber.")

            self.write_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.write_sock.connect((self.ip, self.write_port))
            logger.info("Write connection established with the climatic chamber.")

        except Exception as e:
            logger.error(f"Error connecting: {e}")
            self.disconnect()
            return False
        return True

    def disconnect(self):
        """Disconnects from the read and write ports of the climatic chamber."""
        if self.read_sock:
            self.read_sock.close()
            self.read_sock = None
            logger.info("Read connection closed.")

        if self.write_sock:
            self.write_sock.close()
            self.write_sock = None
            logger.info("Write connection closed.")

    def send_command(self, buffer, is_write=False):
        """Sends a command to the climatic chamber and returns the response."""
        try:
            sock = self.write_sock if is_write else self.read_sock
            sock.send(bytearray(buffer))
            response = sock.recv(1024)
            return response
        except Exception as e:
            logger.error(f"Error sending command: {e}")
            return None

    def read_temperature(self):
        """Reads the temperature measured by the chamber."""
        buffer = [83, 53, 16, 1, 3, 5, 3, 8, 1, 62, 0, 104, 0, 2, 255, 2]
        #buffer = [83, 53, 16, 1, 3, 5, 3, 8, 1, 62, 0, 64, 0, 2, 255, 2]
        response = self.send_command(buffer)

        if response:
            try:
                data_block = response[16:20]
                output = struct.unpack_from('>i', bytearray(data_block))
                ieee = hex(int(str(output)[1:-2]))
                temperature = struct.unpack(">f", int(ieee, 16).to_bytes(4, byteorder="big", signed=True))[0]
                return round(temperature, 10)
            except Exception as e:
                logger.error(f"Error processing temperature response: {e}")
        return None

    def read_humidity(self):
        """Reads the humidity measured by the chamber."""
        #buffer = [83, 53, 16, 1, 3, 5, 3, 8, 1, 62, 0, 160, 0, 2, 255, 2]
        #buffer = [83, 53, 16, 1, 3, 5, 3, 8, 1, 62, 0, 170, 0, 2, 255, 2]
        buffer = [83, 53, 16, 1, 3, 5, 3, 8, 1, 62, 0, 68, 0, 2, 255, 2]
        response = self.send_command(buffer)

        if response:
            try:
                data_block = response[16:20]
                output = struct.unpack_from('>i', bytearray(data_block))
                ieee = hex(int(str(output)[1:-2]))
                humidity = struct.unpack(">f", int(ieee, 16).to_bytes(4, byteorder="big", signed=True))[0]
                return round(humidity, 10)
            except Exception as e:
                logger.error(f"Error processing humidity response: {e}")
        return None

    def read_dewpoint(self, temp, humi):
        """Reads the dewpoint calculated from temperature and humidity."""
        if temp <= 0:
            a = 17.368
            b = 238.88
        else:
            a = 17.966
            b = 247.15

        if humi < 0.01:
            humi = 0.01

        e_s = 6.112 * math.exp((a * temp) / (b + temp))
        e = (humi / 100.0) * e_s

        dewpoint = (b * math.log(e / 6.112)) / (a - math.log(e / 6.112))

        return dewpoint

    def read_dry_air(self):
        """Reads the dry air measured by the chamber."""
        #buffer = [83, 53, 16, 1, 3, 5, 3, 8, 1, 62, 0, 166, 0, 2, 255, 2]
        buffer = [83, 53, 16, 1, 3, 5, 3, 8, 1, 62, 0, 141, 0, 2, 255, 2]
        response = self.send_command(buffer)

        if response:
            try:
                data_block = response[16:20]
                output = struct.unpack_from('>i', bytearray(data_block))
                ieee = hex(int(str(output)[1:-2]))
                dry_air = struct.unpack(">f", int(ieee, 16).to_bytes(4, byteorder="big", signed=True))[0]
                return round(dry_air, 2)
            except Exception as e:
                logger.error(f"Error processing dry air response: {e}")
        return None

    def is_door_closed(self):
        """Checks if the door of the climatic chamber is closed."""
        buffer = [83, 53, 16, 1, 3, 5, 3, 8, 1, 62, 0, 180, 0, 2, 255, 2]  # Adjust the address if necessary
        response = self.send_command(buffer)

        if response:
            try:
                door_status = int.from_bytes(response[16:17], byteorder='big')
                return door_status == 1  # Assuming 1 means closed and 0 means open
            except Exception as e:
                logger.error(f"Error processing door status response: {e}")
        return None

    def set_state(self, state, description):
        """Sets the state of the climatic chamber."""
        buffer = [83, 53, 16, 1, 3, 3, 3, 8, 1, 62, 0, 200, 0, 2, 255, 2]
        buffer[12] = state
        response = self.send_command(buffer, is_write=True)

        if response:
            logger.info(f"State set to {state}: {description}")
            return True
        else:
            logger.error(f"Failed to set state to {state}: {description}")
            return False

    def start_chamber(self):
        """Starts the climatic chamber."""
        return self.set_state(1, "Start Chamber")

    def stop_chamber(self):
        """Stops the climatic chamber."""
        return self.set_state(0, "Stop Chamber")

    def start_dry_air(self):
        """Starts the dry air."""
        return self.set_state(3, "Start Dry Air")

    def stop_dry_air(self):
        """Stops the dry air."""
        return self.set_state(2, "Stop Dry Air")

    def set_temperature_and_slope(self, target_temp, slope):
        """Sets a new target temperature and slope."""
        buffer = [83, 53, 16, 1, 3, 3, 3, 8, 1, 62, 3, 240, 0, 4, 255, 2]

        # Convert temperature and slope to IEEE-754 bytes
        temp_hex = float_to_hex(target_temp)
        slope_hex = float_to_hex(slope)

        temp_bytes = [int(temp_hex[i:i+2], 16) for i in range(2, 10, 2)]
        
        # Ensure slope_hex has a valid value
        if len(slope_hex) < 10:
            slope_hex = '0x' + '00' * ((10 - len(slope_hex)) // 2) + slope_hex[2:]

        slope_bytes = [int(slope_hex[i:i+2], 16) for i in range(2, 10, 2)]

        buffer.extend(temp_bytes + slope_bytes)

        response = self.send_command(buffer, is_write=True)

        if response:
            logger.info(f"Temperature set to {target_temp}°C with a slope of {slope}°C/min")
            return True
        else:
            logger.error("Failed to set temperature and slope.")
            return False

    def go_to_temp(self, temp, slope):
        """Adjusts the chamber to the specified temperature with the given slope."""
        return self.set_temperature_and_slope(temp, slope)

    def go_cold(self):
        """Sets the chamber to -40°C with a default slope of 3°C/min."""
        return self.go_to_temp(-40.0, 3.0)

    def go_room_temp(self):
        """Sets the chamber to 20°C with a default slope of 3°C/min."""
        return self.go_to_temp(20.0, 3.0)

    def go_warm(self):
        """Sets the chamber to 40°C with a default slope of 3°C/min."""
        return self.go_to_temp(40.0, 3.0)

    def maintain_temp(self):
        """Maintains the current chamber temperature."""
        current_temp = self.read_temperature()
        if current_temp is not None:
            logger.info(f"Maintaining current temperature: {current_temp}°C")
            return self.go_to_temp(current_temp, 0.0)
        else:
            logger.error("Failed to read current temperature for maintenance.")
            return False

    def get_status(self):
        """Gets the status of the climatic chamber."""
        buffer = [83, 53, 16, 1, 3, 5, 3, 8, 1, 62, 0, 146, 0, 10, 255, 2]
        response = self.send_command(buffer)

        if response:
            try:
                is_active = int.from_bytes(response[17:18], byteorder='big')
                is_monitoring = int.from_bytes(response[16:17], byteorder='big')
                return {
                    "is_active": bool(is_active),
                    "is_monitoring": bool(is_monitoring),
                }
            except Exception as e:
                logger.error(f"Error processing status: {e}")
        return None

def write_climatic_data_to_influxdb(temperature, dewpoint, humidity):
    """Function to write climatic chamber data to InfluxDB."""
    db_client = InfluxDBClient(url=db_url, token=db_token, org=db_org)

    try:
        bucket = "CliChamber"
        point = Point("temperature").field("chamber_T", temperature).field("chamber_DP", dewpoint).field("chamber_RH", humidity)
        db_client.write_api().write(bucket=bucket, org=db_org, record=point)

        logger.info(f"Climatic data written to InfluxDB: Temperature={temperature}, Dewpoint={dewpoint}, Humidity={humidity}")
    except Exception as e:
        logger.error(f"Failed to write climatic data to InfluxDB: {e}")
    finally:
        db_client.close()

def read_sensors_and_write_to_influxdb():
    """Function to read sensor data and write to InfluxDB."""
    db_client = InfluxDBClient(url=db_url, token=db_token, org=db_org)

    try:
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

            theTime = int(time.time())
            date_time = datetime.fromtimestamp(theTime)

            print("T{} = ".format(i+1), T[i], "°C,  RH{} = ".format(i+1), RH[i], "%, Dew{} = ".format(i+1), DewP[i], "°C @ ", date_time)

            point.append(Point("temperature").field("sensor_T", T[i]).field("sensor_RH", RH[i]).field("sensor_DP", DewP[i]))
            db_client.write_api().write(bucket=db_bucket[i], org=db_org, record=point[i])

    except Exception as e:
        print(f"Failed to read sensors or write to InfluxDB: {e}")
    finally:
        db_client.close()

def main():
    chamber = ClimaticChamber("192.168.1.40", 2000, 2001)

    if not chamber.connect():
        return

    try:
        status = chamber.get_status()
        logger.info(f"Chamber status: {status}")

        temperature = round(chamber.read_temperature(), 2)
        logger.info(f"Measured temperature: {temperature}°C")

        humidity = round(chamber.read_humidity(), 2)
        logger.info(f"Measured humidity: {humidity}%")

        temp_10 = chamber.read_temperature()
        humi_10 = chamber.read_humidity()
        dewpoint = round(chamber.read_dewpoint(temp_10, humi_10), 2)
        logger.info(f"Measured dewpoint: {dewpoint}°C")

        dry_air = chamber.read_dry_air()
        logger.info(f"Measured dry air: {dry_air}%")

        door_closed = chamber.is_door_closed()
        logger.info(f"Door closed: {door_closed}")

        success = chamber.go_to_temp(25.0, 5.0)
        if success:
            logger.info("Temperature and slope successfully configured.")

        chamber.start_chamber()
        chamber.start_dry_air()

        # Write climatic chamber data to InfluxDB
        write_climatic_data_to_influxdb(temperature, dewpoint, humidity)

        # Read sensor data and write to InfluxDB
        read_sensors_and_write_to_influxdb()

        chamber.go_room_temp()
        chamber.maintain_temp()

        chamber.stop_dry_air()
        chamber.stop_chamber()

    finally:
        chamber.disconnect()

if __name__ == "__main__":
    main()
