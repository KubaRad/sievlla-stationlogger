__author__ = 'Kuba Radliński'

import configparser
import sys
import os
import logging
import click
import traceback

import numpy
import pandas as pd
import numpy as np

from datetime import datetime, date
from sievlla_stationlogger.communication.serial_comm import Communicator
from sievlla_stationlogger.communication.davis import NULL_DATE_TIME, DavisCommunicator, WindDirMeasurementsUnit

_CSV_COLUMN_NAMES = ["DATE_TIME","TEMP","TEMPMIN","TEMPMAX","PRESS","PRESSSEA","HUM","WIND_SPEED","WIND_DIR","WIND_GUST_SPEED","WIND_GUST_DIR","RAIN","RAIN_RATE"]

class WrongLogLevel(configparser.Error):
    pass


class WrongNumericValue(configparser.Error):
    pass


class WrongBaudRate(configparser.Error):
    pass


class WrongRainCollector(configparser.Error):
    pass

class WrongDataFile(configparser.Error):
    pass

class Configuration:

    def __init__(self, configfile, logging_system):
        self.logging_system = logging_system
        self.configfile = configfile
        self.config = configparser.ConfigParser(interpolation=None)
        with open(self.configfile) as f:
            self.config.read_file(f)

    def read(self):
        self._read_general_section()
        self._read_station_comm_section()
        self._read_data_file_section()

    def _read_general_section(self):
        self.log_level = self.config.get('General', 'loglevel', fallback="info")
        self.station_name = self.config.get('General', 'StationName', fallback='TESTING')
        self.log_file = self.config.get("General", "logfile", fallback="")
        self.time_zone = self.config.get('General', 'Timezone', fallback='CET')
        self.collector_type = self.config.get('General', 'CollectorType', fallback='RAIN_02MM')
        possible_rain_collectors = ("RAIN_001IN", "RAIN_01MM", "RAIN_02MM")
        if self.collector_type.upper() not in possible_rain_collectors:
            raise WrongRainCollector("CollectorType must be one of " + ", ".join(possible_rain_collectors))
        possible_log_levels = ("ERROR", "WARNING", "INFO", "DEBUG")
        if self.log_level.upper() not in possible_log_levels:
            raise WrongLogLevel("loglevel must be one of " + ", ".join(possible_log_levels))
        try:
            self.station_altitude = float(self.config.get('General', 'StationAltitude', fallback='0.0'))
        except ValueError:
            raise WrongNumericValue("StationAltitude must be valid float number")

    def _read_station_comm_section(self):
        self.port_name = self.config.get('StationComm', 'Port', fallback='COM1')
        valid_baud_rates = ("300", "600", "1200", "1800", "2400", "4800", "9600", "19200")
        try:
            self.baud_rate = int(self.config.get('StationComm', 'Baudrate', fallback=str(Communicator.BAUDRATE)))
        except ValueError:
            raise WrongBaudRate("Baudrate shoud be one of "+", ".join(valid_baud_rates))
        if self.baud_rate not in [int(x) for x in valid_baud_rates]:
            raise WrongBaudRate("Baudrate shoud be one of " + ", ".join(valid_baud_rates))

    def _read_data_file_section(self):
        self.data_file = self.config.get('File', 'DataFile', fallback="C:\\TEMP\\meteologger-new\\data\\meteo-data.csv")
        splitted_data_file_path = os.path.split(self.data_file)
        if splitted_data_file_path[0] is None:
            raise WrongDataFile("Data file should contains full path")


class Logging:
    def __init__(self):
        self.logger = logging.getLogger("sievlla_stationlogger")
        self.stdout_handler = logging.StreamHandler()
        self.logger.addHandler(self.stdout_handler)

    def setup_logger(self, configuration):
        self.logger.setLevel(configuration.log_level.upper())
        if configuration.log_file:
            self.logger.removeHandler(self.stdout_handler)
            self.logger.addHandler(logging.FileHandler(configuration.log_file))

    def log_start_of_execution(self):
        self.logger.info("Starting stationlogger, " + datetime.today().isoformat())

    def log_end_of_execution(self):
        self.logger.info("Finished stationlogger, " + datetime.today().isoformat())

class StationLogger:
    def __init__(self, configfile):
        self.configfile = configfile
        self.logging_system = Logging()
        self._stored_data = None

    def run(self):
        try:
            self.configuration = Configuration(self.configfile, self.logging_system)
            self.configuration.read()
            self.logging_system.setup_logger(self.configuration)
            self.logging_system.log_start_of_execution()
            self._init_data_path()
            self._read_stored_data()
            records = self._read_archiveRecords()
            self._store_records(records)
            self.logging_system.log_end_of_execution()
        except Exception as e:
            self.logging_system.logger.error(e)
            self.logging_system.logger.debug(traceback.format_exc())
            raise click.ClickException(str(e))

    def _init_data_path(self):
        data_path = os.path.split(self.configuration.data_file)[0]
        if not os.path.exists(data_path):
            self.logging_system.logger.info('No folder %s found. Creating...' % data_path)
            os.makedirs(data_path)
            self.logging_system.logger.info('Folder %s created.' % data_path)

    def _read_stored_data(self):
        if not os.path.exists(self.configuration.data_file):
            self._stored_data = None
        else:
            self._stored_data = pd.read_csv(self.configuration.data_file, parse_dates=[0], names=_CSV_COLUMN_NAMES,
                usecols=_CSV_COLUMN_NAMES, index_col=0, header=0,
                dtype={'value': np.float64})

    def _find_last_stored_measurement(self):
        if self._stored_data is None:
            return NULL_DATE_TIME
        return self._stored_data.index[-1]

    def _read_archiveRecords(self):
        lsm = self._find_last_stored_measurement()
        self.logging_system.logger.info('Last stored record: %s' % (lsm.__str__()))
        communicator = Communicator(self.configuration.port_name, baud_rate=self.configuration.baud_rate)
        communicator.open_port()
        davis_communicator = DavisCommunicator(communicator, self.logging_system.logger, wind_dir_measurements_unit=WindDirMeasurementsUnit.DEG, altitude=self.configuration.station_altitude)
        davis_communicator.wake_up()
        davis_communicator.test_comm()
        records = davis_communicator.get_archive_data(lsm)
        communicator.close_port()
        return records

    def _store_records(self, records):
        if len(records) > 0:
            self.logging_system.logger.info('Received %d records' % len(records))
            index = [x.datetime() for x in records ]
            data = [self._convert_record_to_dict(x) for x in records]
            new_data_frame =  pd.DataFrame(data, index=index, columns=_CSV_COLUMN_NAMES[1:])
            finall_data_frame = new_data_frame if self._stored_data is None else pd.concat([self._stored_data, new_data_frame])
            finall_data_frame.to_csv(self.configuration.data_file, na_rep='NA', index_label='DATE_TIME')
            pass
        else:
            self.logging_system.logger.info('No new records received')

    def _convert_record_to_dict(self, record):
        return {"TEMP": round(record.out_temp, 1) if record.out_temp is not None else numpy.nan,
                "TEMPMIN": round(record.low_out_temp, 1) if record.low_out_temp is not None else numpy.nan,
                "TEMPMAX": round(record.hi_out_temp, 1) if record.hi_out_temp is not None else numpy.nan,
                "PRESS": round(record.barometer, 1) if record.barometer is not None else numpy.nan,
                "PRESSSEA": round(record.barometer_sea, 1) if record.barometer_sea is not None else numpy.nan,
                "HUM": round(record.outside_humidity, 1) if record.outside_humidity is not None else numpy.nan,
                "WIND_SPEED": round(record.avg_wind_speed, 1) if record.avg_wind_speed is not None else numpy.nan,
                "WIND_DIR": round(record.direction_prev_wind, 1) if record.direction_prev_wind is not None else numpy.nan,
                "WIND_GUST_SPEED": round(record.high_wind_speed, 1) if record.high_wind_speed is not None else numpy.nan,
                "WIND_GUST_DIR": round(record.direction_hi_wind, 1) if record.direction_hi_wind is not None else numpy.nan,
                "RAIN": round(record.rainfall, 1) if record.rainfall is not None else numpy.nan,
                "RAIN_RATE": round(record.high_rain_rate, 1) if record.high_rain_rate is not None else numpy.nan}

@click.command()
@click.argument("configfile")
@click.version_option(prog_name="stationlogger")
def main(configfile):
    """Read metereological data from Davis Console"""
    StationLogger(configfile).run()


if __name__ == '__main__':
    sys.exit(main())