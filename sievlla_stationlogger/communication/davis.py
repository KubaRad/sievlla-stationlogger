import struct
import array
import logging
from datetime import datetime, date, time
from enum import Enum
from math import isnan, exp

NULL_DATE_TIME = datetime(1, 1, 1, 0, 0, 0)

class RainCollector(Enum):
    RAIN_001IN = 0.01
    RAIN_01MM = 0.10
    RAIN_02MM = 0.20


class RainMeasurementsUnit(Enum):
    MM = "mm"
    IN = "in"


class TemperatureMeasurementsUnit(Enum):
    F = "f"
    C = "c"


class WindSpeedMeasurementsUnit(Enum):
    M_S = "m/s"
    K = "???"


class WindDirMeasurementsUnit(Enum):
    DEG = "°"
    NAMES = "names"


class AirPressureMeasurementsUnit(Enum):
    IN_HG = "inHg"
    HPA = "hPa"


class DavisCommunicator:
    _CRC_TABLE = [
        0x0, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
        0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
        0x1231, 0x210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
        0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
        0x2462, 0x3443, 0x420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
        0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
        0x3653, 0x2672, 0x1611, 0x630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
        0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
        0x48c4, 0x58e5, 0x6886, 0x78a7, 0x840, 0x1861, 0x2802, 0x3823,
        0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
        0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0xa50, 0x3a33, 0x2a12,
        0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
        0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0xc60, 0x1c41,
        0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
        0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0xe70,
        0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
        0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
        0x1080, 0xa1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
        0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
        0x2b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
        0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
        0x34e2, 0x24c3, 0x14a0, 0x481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
        0x26d3, 0x36f2, 0x691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
        0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
        0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x8e1, 0x3882, 0x28a3,
        0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
        0x4a75, 0x5a54, 0x6a37, 0x7a16, 0xaf1, 0x1ad0, 0x2ab3, 0x3a92,
        0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
        0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0xcc1,
        0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
        0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0xed1, 0x1ef0]

    _NULL_DATE = date(1, 1, 1)
    _WIND_DIRECTIONS = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW',
                        'NNW']
    _WIND_DIRECTIONS_DEG_MAP = {'N': 0.0,
                                'NNE': 22.5,
                                'NE': 45.0,
                                'ENE': 67.5,
                                'E': 90.0,
                                'ESE': 112.5,
                                'SE': 135,
                                'SSE': 157.5,
                                'S': 180.0,
                                'SSW': 202.5,
                                'SW': 225.0,
                                'WSW': 247.5,
                                'W': 270.0,
                                'WNW': 292.5,
                                'NW': 315.0,
                                'NNW': 337.5}
    _MEAS_NAMES_TO_COLUMN_NAMES = {"out_temp": "",
                                   "hi_out_temp": "",
                                   "low_out_temp": "",
                                   "rainfall": "",
                                   "high_rain_rate": "",
                                   "barometer": "",
                                   "solar_radiation": "",
                                   "no_wind_samples": "",
                                   "inside_temp": "",
                                   "inside_humidity": "",
                                   "outside_humidity": "",
                                   "avg_wind_speed": "",
                                   "high_wind_speed": "",
                                   "direction_hi_wind": "",
                                   "direction_prev_wind": "",
                                   "barometer_sea": ""}
    _RETRIES = 3  # number of retries

    def __init__(self, communicator, logger, rain_collector=RainCollector.RAIN_02MM,
                 rain_measurements_unit=RainMeasurementsUnit.MM,
                 temperature_measurements_unit=TemperatureMeasurementsUnit.C,
                 wind_speed_measurements_unit=WindSpeedMeasurementsUnit.M_S,
                 air_pressure_measurements_unit=AirPressureMeasurementsUnit.HPA,
                 wind_dir_measurements_unit=WindDirMeasurementsUnit.NAMES,
                 retries_attempt=_RETRIES,
                 altitude=0.0):
        super().__init__()
        self._communicator = communicator
        self._logger = logger
        self._rain_collector = rain_collector
        self._rain_measurements_unit = rain_measurements_unit
        self._temperature_measurements_unit = temperature_measurements_unit
        self._wind_speed_measurements_unit = wind_speed_measurements_unit
        self._air_pressure_measurements_unit = air_pressure_measurements_unit
        self._wind_dir_measurements_unit = wind_dir_measurements_unit
        self._retries_attempt = retries_attempt
        self._altitude = altitude

    def wake_up(self):
        for i in range(self._retries_attempt):
            self._communicator.write(b'\n')
            rcv = self._communicator.read(2)
            if b'\n\r' == rcv:
                return True
        return False

    def get_station_code(self):
        self._communicator.write(b'WRD')
        self._communicator.write(struct.pack('2b', 0x12, 0x4d))
        self._communicator.write(b'\n')
        self._communicator.wait_for_ack()
        rcv = self._communicator.read(1)
        els = struct.unpack('1b', rcv)
        return els[0]

    def test_comm(self):
        self._communicator.write(b'TEST\n')
        rcv = self._communicator.read(8)
        return rcv == '\n\rTEST\n\r'

    def get_time(self):
        self._communicator.write(b'GETTIME')
        self._communicator.write(b'\n')
        self._communicator.wait_for_ack()
        rcv = self._communicator.read(6)
        crcv = self._communicator.read(2)
        el = struct.unpack_from('>H', array.array('B', crcv))
        crc_rcv = el[0]
        crc_calc = self._calculate_crc(array.array('B', rcv))
        if crc_rcv != crc_calc:
            return None
        else:
            el = struct.unpack_from('6B', array.array('B', rcv))
            second = el[0]
            minute = el[1]
            hour = el[2]
            day = el[3]
            month = el[4]
            year = 1900 + el[5]
            return datetime(year, month, day, hour, minute, second)

    def set_time(self, dt):
        self._communicator.write(b'SETTIME')
        self._communicator.write(b'\n')
        self._communicator.wait_for_ack()
        ds = struct.pack("6B", dt.second, dt.minute, dt.hour, dt.day, dt.month, dt.year - 1900)
        buf = array.array('B', ds)
        crc_buf = DavisCommunicator._calculate_crc(buf)
        self._communicator.write(buf)
        self._communicator.write(struct.pack('>H', crc_buf))
        self._communicator.wait_for_ack()

    def get_archive_data(self, last_stored_date):
        self._communicator.write(b'DMPAFT')
        self._communicator.write(b'\n')
        self._communicator.wait_for_ack()
        buf = DavisCommunicator._date_time_to_array(last_stored_date)
        crc_buf = DavisCommunicator._calculate_crc(buf)
        self._communicator.write(buf)
        self._communicator.write(struct.pack('>H', crc_buf))
        self._communicator.wait_for_ack()
        rcv = self._communicator.read(4)
        crcv = self._communicator.read(2)
        el = struct.unpack_from('>H', array.array('B', crcv))
        crc_rcv = el[0]
        el = struct.unpack_from('<2H', array.array('B', rcv))
        num_pages = el[0]
        valid_record = el[1]
        calculated_crc = DavisCommunicator._calculate_crc(array.array('B', rcv))
        self._logger.info('pages %d starting from record %d' % (num_pages, valid_record))
        self._communicator.write_ack()
        # prev = -1
        records = []
        for i in range(num_pages):
            page_data = self._communicator.read(267)
            page = array.array('B', page_data[0:265])
            crc_data = array.array('B', page_data[265:267])
            crc = struct.unpack_from('>H', crc_data)[0]
            calc_crc = DavisCommunicator._calculate_crc(page)
            if calc_crc == crc:
                self._communicator.write_ack()
                el = struct.unpack_from('B52s52s52s52s52s', page)
                # seq_byte = el[0]
                # if seq_byte == 255:
                #     prev = -1
                # else:
                #     prev = seq_byte
                for j in range(5):
                    if (i == 0) & (j < valid_record):
                        pass
                    elif self._is_valid_record(el[1 + j]):
                        data_record = self._create_archive_data_from_bytes(el[1 + j])
                        record_timestamp = data_record.datetime()
                        if record_timestamp > last_stored_date:
                            records.append(data_record)
                            self._logger.info('Adding record for: %s' % record_timestamp)
        records.sort(key=lambda x: x.datetime())
        return records

    def _is_valid_record(self, s):
        el = struct.unpack('HH', s[0:4])
        if (el[0] == 65535) | (el[1] == 65535):
            return False
        else:
            dt = self._decode_date(el[0])
            if dt is None:
                return False
            tm = self._decode_time(el[1])
            if tm is None:
                return False
            return True

    def _create_archive_data_from_bytes(self, s):
        vl = struct.unpack('2H3h5Hh8BH20B', s)
        dt = self._decode_date(vl[0])
        record_date = None if (dt.year == 0) & (dt.month == 0) & (dt.day == 0) else dt

        record_time = self._decode_time(vl[1])

        record = MeteoRecord(record_date, record_time)

        record.out_temp = None if vl[2] == 32767 else self._convert_temp(vl[2])
        record.hi_out_temp = None if vl[3] == -32768 else self._convert_temp(vl[3])
        record.low_out_temp = None if vl[4] == 32767 else self._convert_temp(vl[4])
        record.rainfall = self._convert_rain(vl[5])
        record.high_rain_rate = self._convert_rain(vl[6])
        record.barometer = None if vl[7] == 0 else self._convert_air_pressure(vl[7])
        record.solar_radiation = None if vl[8] == 32767 else vl[8]
        record.no_wind_samples = None if vl[9] == 0 else vl[9]
        record.inside_temp = None if vl[10] == 32767 else self._convert_temp(vl[10])
        record.inside_humidity = None if vl[11] == 255 else vl[11]
        record.outside_humidity = None if vl[12] == 255 else vl[12]
        record.avg_wind_speed = None if vl[13] == 255 else self._convert_wind_speed(vl[13])
        record.high_wind_speed = None if vl[14] == 0 else self._convert_wind_speed(vl[14])
        record.direction_hi_wind = None if vl[15] == 255 else self._convert_wind_dir(vl[15])
        record.direction_prev_wind = None if vl[16] == 255 else self._convert_wind_dir(vl[16])
        record.barometer_sea = None if (record.barometer is None) or (record.inside_temp is None) \
            else DavisCommunicator._barometric_formula(record.inside_temp, record.barometer, 0.0 - self._altitude)
        return record

    @staticmethod
    def _to_reverse_bytes_block(i):
        return [(i >> 8) & 0xff, i & 0xff]

    @staticmethod
    def _encode_time(tm):
        return tm.hour * 100 + tm.minute

    def _decode_time(self, i):
        h = i // 100
        m = i - (h * 100)
        if (h < 0 & h > 23) | (m < 0 & m > 59):
            self._logger.error("Wrong time h=%d m=%d".format(h, m))
            return None
        return time(h, m).replace(second=0, microsecond=0)

    @staticmethod
    def _encode_date(dt):
        return 0 if dt == DavisCommunicator._NULL_DATE else dt.day + dt.month * 32 + (dt.year - 2000) * 512

    def _decode_date(self, i):
        y = (i & 0xfe00) >> 9
        m = (i & 0x1e0) >> 5
        d = (i & 0x1f)
        if (y < 0 & y > 99) | (m < 1 & m > 12) | (d < 1 & d > 31):
            self._logger.error("Wrong date y=%d m=%d d=%d".format(y, m, d))
            return None
        return date(y + 2000, m, d)

    @staticmethod
    def _date_time_to_array(dt):
        s = struct.pack('<2H', DavisCommunicator._encode_date(dt.date()), DavisCommunicator._encode_time(dt.time()))
        return array.array('B', s)

    @staticmethod
    def _print_hex_byte_table(block):
        result = "["
        first = True
        for b in block:
            if not first:
                result += ","
            else:
                first = False
            result += hex(b)
        return result + "]"

    @staticmethod
    def _calculate_crc(block):
        accu = 0
        for b in block:
            accu = DavisCommunicator._CRC_TABLE[(accu >> 8) ^ b] ^ ((accu & 0x00ff) << 8)
        return accu

    def _convert_wind_dir(self, wv):
        if 0 <= wv <= 15:
            wind_dir_name = DavisCommunicator._WIND_DIRECTIONS[wv]
            return wind_dir_name if self._wind_dir_measurements_unit == WindDirMeasurementsUnit.NAMES \
                else DavisCommunicator._WIND_DIRECTIONS_DEG_MAP[wind_dir_name]
        else:
            return None

    def _convert_rain(self, v):
        if self._rain_measurements_unit == RainMeasurementsUnit.MM:
            if self._rain_collector == RainCollector.RAIN_001IN:
                transform = 0.01 * 25.45
            elif self._rain_collector == RainCollector.RAIN_01MM:
                transform = 0.1
            elif self._rain_collector == RainCollector.RAIN_02MM:
                transform = 0.2
            else:
                transform = None
            return v * transform if transform is not None else None
        elif self._rain_measurements_unit == RainMeasurementsUnit.IN:
            if self._rain_collector == RainCollector.RAIN_001IN:
                transform = 0.01
            elif self._rain_collector == RainCollector.RAIN_01MM:
                transform = 0.1 / 25.45
            elif self._rain_collector == RainCollector.RAIN_02MM:
                transform = 0.2 / 25.45
            else:
                transform = None
            return v * transform if transform is not None else None
        else:
            return None

    def _convert_wind_speed(self, v):
        return v if self._wind_speed_measurements_unit == WindSpeedMeasurementsUnit.K else v * 0.44704

    def _convert_air_pressure(self, v):
        air_press_in_hg = v / 1000.0
        return air_press_in_hg if self._air_pressure_measurements_unit == AirPressureMeasurementsUnit.IN_HG \
            else air_press_in_hg * 33.86389

    def _convert_temp(self, v):
        temp_in_fahrenheit = v / 10.0
        return temp_in_fahrenheit if self._temperature_measurements_unit == TemperatureMeasurementsUnit.F \
            else 5.0 / 9.0 * (temp_in_fahrenheit - 32)

    # code below is copied from project pthelma https://github.com/openmeteo/pthelma/blob/master/pthelma/meteocalcs.py
    # Gravitational acceleration at phi=45deg m^2/s
    _G0 = 9.80665
    # Universal gas constant for air in N.m/(mol.K)
    _RS = 8.31432
    # Molar mass of Earth's air in kg/mol
    _M_AIR = 0.0289644

    @staticmethod
    def _barometric_formula(temp, barometric_pressure, hdiff):
        """Return the barometric pressure at a level
        h if the pressure Pb at an altutde hb is given for
        atmospheric temperature T, according to the
        barometric formula. hdiff is h-hb.
        """
        for v in (temp, barometric_pressure, hdiff):
            if isnan(v):
                return float('NaN')
        temp += 273.75
        return barometric_pressure * exp((-DavisCommunicator._G0 * DavisCommunicator._M_AIR * hdiff)
                                         / (DavisCommunicator._RS * temp))

class MeteoRecord(object):
    def __init__(self, record_date, record_time):
        self.record_date = record_date
        self.record_time = record_time
        self.out_temp = None
        self.low_out_temp = None
        self.hi_out_temp = None
        self.outside_humidity = None
        self.barometer = None
        self.barometer_sea = None
        self.avg_wind_speed = None
        self.direction_prev_wind = None
        self.high_wind_speed = None
        self.direction_hi_wind = None
        self.rainfall = None
        self.high_rain_rate = None
        self.solar_radiation = None
        self.no_wind_samples = None
        self.inside_temp = None
        self.inside_humidity = None

    def datetime(self):
        return datetime.combine(self.record_date, self.record_time)

