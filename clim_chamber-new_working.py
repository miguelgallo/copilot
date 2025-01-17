import socket
import struct
import logging
import time
from os import getcwd
from datetime import datetime

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
        response = self.send_command(buffer)

        if response:
            try:
                data_block = response[16:20]
                output = struct.unpack_from('>i', bytearray(data_block))
                ieee = hex(int(str(output)[1:-2]))
                temperature = struct.unpack(">f", int(ieee, 16).to_bytes(4, byteorder="big", signed=True))[0]
                return round(temperature, 2)
            except Exception as e:
                logger.error(f"Error processing temperature response: {e}")
        return None

    def set_temperature_and_slope(self, target_temp, slope):
        """Sets a new target temperature and slope."""
        buffer = [83, 53, 16, 1, 3, 3, 3, 8, 1, 62, 3, 240, 0, 4, 255, 2]

        # Convert temperature and slope to IEEE-754 bytes
        temp_hex = float_to_hex(target_temp)
        slope_hex = float_to_hex(slope)

        temp_bytes = [int(temp_hex[i:i+2], 16) for i in range(2, 10, 2)]
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
        #return self.set_temperature_and_slope(-40.0, 3.0)
        return self.go_to_temp(-40.0, 3.0)

    def go_room_temp(self):
        """Sets the chamber to 20°C with a default slope of 3°C/min."""
        #return self.set_temperature_and_slope(20.0, 3.0)
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

# Usage example
def main():
    chamber = ClimaticChamber("192.168.1.40", 2000, 2001)

    if not chamber.connect():
        return

    try:
        status = chamber.get_status()
        logger.info(f"Chamber status: {status}")

        temperature = chamber.read_temperature()
        logger.info(f"Measured temperature: {temperature}°C")

        success = chamber.go_to_temp(25.0, 5.0)
        if success:
            logger.info("Temperature and slope successfully configured.")

        #chamber.go_cold()
        chamber.go_room_temp()
        #chamber.go_warm()
        chamber.maintain_temp()

    finally:
        chamber.disconnect()

if __name__ == "__main__":
    main()
