
import struct
import serial
import logging

__author__ = u'Kuba Radliński'

_logger = logging.getLogger(__name__)


class Communicator(object):
    CR = 0x0D
    LF = 0x0A
    ACK = 0x06
    NAK = 0x21
    CANCEL = 0x18

    TIMEOUT = 3  # 3s timeout
    BAUDRATE = 19200

    def __init__(self, port_name, timeout=TIMEOUT, baud_rate=BAUDRATE):
        self._port_name = port_name
        self._timeout = timeout
        self._baud_rate = baud_rate
        self._port = None

    def open_port(self):
        self._port = serial.Serial(self._port_name, timeout=self._timeout, baudrate=self._baud_rate)
        return

    def close_port(self):
        self._port.close()
        return

    def wait_for_ack(self):
        rcv = self._port.read(1)
        els = struct.unpack('1b', rcv)
        if self.ACK == els[0]:
            return True
        return False

    def write_ack(self):
        self._port.write(struct.pack('B', self.ACK))
        return

    def write(self, bytes_to_send):
        self._port.write(bytes_to_send)
        return

    def read(self, num_bytes):
        return self._port.read(num_bytes)
