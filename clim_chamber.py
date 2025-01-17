"""
Module for remote control of climatic chamber.

Name: clim_chamber.py
Author: pszydlik
Date: 13/03/23
Brief: Module for remote control of climatic chamber.
"""
import socket
import logging
from os import getcwd
from datetime import datetime


DEBUG = False
LOG_FILE = False
FILE_NAME_FORMAT = '%Y-%m-%d_%H-%M-%S_climatic_chamber.log'

VAR_DICT = {
    "start": "Marche_Arret",
    "cancel": "Annulation_essai",
    "program": "Programme en cours",
    "error": "Defaut",
    "time_left": "TpsTot_Restant",
    "pause": "PauseProg",
    "running": "CycleEnCours",
    "temperature": "ConsCuve", # temperature set point
    "humidity": "ConsHum",    # humidity set point
    "temperature_meas": "TCuve", # temperature measurement
    "temperature_air":"TAir",
    "temperature_probe":"TProduit",
    "temperature_probe_2": "TProbe2",
    "humidity_meas": "HumCuve",
    "segment_time_left": "TempRestSeg",
    "date": "Date",
    "time": "Heure",
    "comment": "Commentaires",
    "total_time": "TpsTot_Ecoulé",
    "cycle_name": "Nom_Essai",
    "chamber_name": "NomEquipement",
    "interface": "HMI_Disable",
    "door_closed": "CTPORTE",
    "analog_input_1":"NCab_EA1",
    "analog_input_2":"NCab_EA2",
    "analog_input_3":"NCab_EA3",
    "analog_input_4":"NCab_EA4",
    "analog_output_1":"NCab_SA1",
    "analog_output_2":"NCab_SA2",
    "digital_input_1":"NCab_ELc1",
    "digital_input_2":"NCab_ELc2",
    "digital_input_3":"NCab_ELc3",
    "digital_input_4":"NCab_ELc4",
    "relay_1":"NCab_RLC1",
    "relay_2":"NCab_RLC2",
    "relay_3":"NCab_RLC3",
    "relay_4":"NCab_RLC4",
    "dewpoint":"TrVaisa",
    "dry_air":"VAzote", # dewpoint measurements
    "humidity_sens":"RHDirect", # humidity measurement
    # Below the shortcuts for lazy people like me
    "t": "ConsCuve",
    "h": "ConsHum",
    "t_meas": "TCuve",
    "t_air":"TAir",
    "t_probe":"TProduit",
    "t_probe2": "TProbe2",
    "h_meas": "HumCuve",
    "h_sens":"RHDirect",
    "t_dew":"TrVaisa"
}

STATE_DICT = {
    "start": "Marche_Arret=1",
    "stop": "Marche_Arret=0",
    "pause": "PauseProg=1",
    "continue": "PauseProg=0",
    "lock_UI": "HMI_Disable=1",
    "unlock_UI": "HMI_Disable=0",
}

_READ_ONLY = [
    "running",
    "temperature_meas",
    "temperature_probe",
    "temperature_probe_2",
    "humidity_meas",
    "TrVaisa",
    "RHDirect",
    "segment_time_left",
    "date",
    "time",
    "total_time",
    "door_closed",
    "digital_input_1",
    "digital_input_2",
    "digital_input_3",
    "digital_input_4",
    "analog_input_1",
    "analog_input_2",
    "analog_input_3",
    "analog_input_4"
]

logger = logging.getLogger('clim_chamber')
logger.setLevel(logging.INFO)
loggingString = '%(levelname)s:  %(message)s'

if DEBUG:
    logger.setLevel(logging.DEBUG)
    loggingString = '%(filename)20s:%(lineno)3s --- %(levelname)s:  %(message)s'

formatter = logging.Formatter(loggingString)

if LOG_FILE:
    log_file = ""
    now = datetime.now()
    filename = now.strftime(FILE_NAME_FORMAT)
    log_file = getcwd() + '/logs/' + filename
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG if DEBUG else logging.INFO)
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.debug('Creating an instance of logger.')

class TCPClientCC:
    """Class for communicating with TCP/IP server."""

    ip_address = ""
    port = ""

    def __init__(self, ip_address, port):
        """Brief:  Initializer with ip_address and port to TCP server."""
        self.ip_address = ip_address
        self.port = port

    def connect(self):
        """Brief:  Establish connection with server."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.ip_address, self.port))
            return True
        except Exception as e:   # If there is an error, print it out.
            logger.error(e)
            return False

    def close(self):
        """Brief:  Close the connection with server."""
        if hasattr(self, 'sock'):
            self.sock.close()
            delattr(self, "sock")

    def send(self, message):
        """Brief:  Sends a message to the climatic chamber and returns a response."""
        message += '\r\n'
        if hasattr(self, 'sock'):
            message_bytes = bytes(message, 'ascii')
            logger.debug(f"Sending {message_bytes} ")
            self.sock.send(message_bytes)
        else:
            return False
            logger.warning("Send not possible")
        logger.debug("Send completed")
        return self.receive()

    def receive(self):
        """Brief:  Receives the response of the server."""
        if hasattr(self, 'sock'):
            logger.debug("Reading from client")
            response = self.sock.recv(1024)
            logger.debug(f"Received {response}")
            return response
        logger.error("No connection")
        return False

    def send_command(self, command):
        """Brief:  Send the command to the server."""
        response = self.send(command)
        if response == b'NA\r\n':
            logger.error('Incorrect command')
            return False
        return response


class ClimaticChamberController:
    """Class implementing basic control interface for climatic chamber."""

    if_transition = False

    def __init__(self, host, port, emulate):
        """Brief:  Initialize the class and connect to the TCP Client."""
        self.client = TCPClientCC(host, port)
        self.emulate = emulate
        self.emulate_status = {}
        self.emulate_status['temperature_meas'] = '20'

    def connect(self):
        """Connect with the TCP server of climatic chamber."""
        if(self.emulate): return True
        if not self.client.connect():
            return False
        return True

    def check_status(self):
        """
        Brief: Updates the status of climatic chamber.

        Return: Dictionary containing the value of
                - if_running    - <bool>
                - if_pause      - <bool>
                - if_transition - <bool>
                - if_closed     - <bool>
                - error value   - <str>
        """
        if_running = self.read_value('start')
        if_pause = self.read_value('pause')
        error = self.read_value('error')
        if_closed = self.read_value('door_closed')

        status_dict = {
            'if_running': if_running == '1',
            'if_pause': if_pause == '1',
            'if_transition': self.if_transition,
            'if_closed': if_closed == '1',
            'error': error
        }

        return status_dict

    def read_value(self, variable):
        """
        Brief:  Read the value of the variable predefined by the VAR_DICT.

        Return: String representing the read value or False.
        """
        if variable not in VAR_DICT:
            logger.warning(f'Unrecognised variable {variable}')
            return False
        command = f'?{VAR_DICT[variable]}'
        if(self.emulate):
            if(variable in self.emulate_status):
                return self.emulate_status[variable]
            else:
                return "0"
        response = self.client.send_command(command)
        if not response: return False
        return response.decode('ascii').split("=")[-1].rstrip()

    def set_value(self, variable, value):
        """
        Brief:  Set the value of the variable predefined by the VAR_DICT.

        Return: String representing the set value or False.
        """
        if variable not in VAR_DICT:
            logger.warning(f'Unrecognised variable {variable}')
            return False
        elif variable in _READ_ONLY:
            logger.warning(f'Variable {variable} is READ ONLY!')
            return False
        elif variable == 'temperature':
            if '>' in str(value):
                self.if_transition = True
                logger.debug('Transitioning enables')
            else:
                self.if_transition = False
                logger.debug('Transitioning disabled')
        command = f'{VAR_DICT[variable]}={value}'
        if(self.emulate):
            self.emulate_status[variable] = value
            return "0"
        response = self.client.send_command(command)
        if not response:
            return False
        return response.decode('ascii').split("=")[-1].rstrip()

    def set_state(self, state):
        """
        Brief:  Set one of the states predefined by the STATE_DICT.

        Return: String representing the set value or False
        """
        if state not in STATE_DICT:
            logger.warning(f'Unrecognised state {state}')
            return False
        if(self.emulate): return "0"
        response = self.client.send_command(STATE_DICT[state])
        if not response:
            return False
        return response.decode('ascii').split("=")[-1].rstrip()

    def go_to_temp(self, temp, slope):
        """
        Brief:  Brings the chamber to the selected temperature from any
                temperature. With chosen slope limit.

        Return: Estimated time (in sec) until temperature reach setpoint.
        """
        temp_meas = float(self.read_value('temperature_meas'))
        required_time = int(60.0*abs(float(temp)-temp_meas)/slope)
        if required_time == 0: required_time = 1
        if(self.emulate):
            required_time = 5
            self.emulate_status['temperature_meas'] = temp
        logger.info(f'Setting the temperature to {temp} in a time of {required_time} s')
        target_value = f'{temp}>{required_time}'
        response = self.set_value('temperature', target_value)
        if response == b'NA':
            return False
        return required_time

    def go_cold(self):
        """
        Brief:  Brings the chamber to -40 degrees from any temperature.
                Slope of 3degC/min.

        Return: Estimated time (in sec) until temperature reach setpoint.
        """
        logger.info("Mockup of the go cold function for test purposes")
        return self.go_to_temp(7, 3)
        #return self.go_to_temp(-35, 3)

    def go_room_temp(self):
        """
        Brief:  Brings the chamber to +20 degrees from any temperature.
                Slope of 3degC/min.

        Return: Estimated time (in sec) until temperature reach setpoint.
        """
        return self.go_to_temp(20, 3)

    def go_warm(self):
        """
        Brief:  Brings the chamber to +40 degrees from any temperature.
                Slope of 3degC/min.

        Return: Estimated time (in sec) until temperature reach setpoint.
        """
        return self.go_to_temp(40, 3)

    def maintain_temp(self):
        """
        Brief:  Maintain the current temperature.

        Return: Response from the climatic chamber (echo or NA)
        """
        temp_meas = round(float(self.read_value('temperature_meas')), 1)
        return self.set_value('temperature', temp_meas)


