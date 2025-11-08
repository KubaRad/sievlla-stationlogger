import logging
import sys
import traceback

import click
import pytz

from datetime import datetime
from sievlla_stationlogger.communication.serial_comm import Communicator
from sievlla_stationlogger.communication.davis import DavisCommunicator

@click.command()
@click.option(
    "--baudrate", is_flag=False, default="19200", help="Baudrate",
)
@click.option(
    "--check", is_flag=False, default=False, help="Check",
)
@click.option(
    "--timezone", is_flag=False, default="Etc/GMT-1", help="Timezone",
)
@click.option(
    "--timedelta", is_flag=False, default=5, help="Timedelta",
)
@click.option(
    "--settime", is_flag=False, default=False, help="Settime",
)
@click.argument("portname", default="/dev/ttyUSB0")
@click.version_option( prog_name="checktime")
def main(baudrate, check, timezone, timedelta, settime, portname):
    """Insert meteorological logger data to Enhydris"""
    CheckTime(baudrate, check, timezone, timedelta, settime, portname).run()


class CheckTime:
    def __init__(self, baudrate, check, timezone, timedelta, settime, portname):
        self.baudrate = int(baudrate)
        self.portname = portname
        self.check = bool(check)
        self.logging_system = Logging()
        self.timezone = timezone
        self.timedelta = timedelta
        self.settime = settime

    def run(self):
        try:
            current_time = datetime.now(pytz.timezone(self.timezone))
            station_time = self._process_check_time().replace(tzinfo=pytz.timezone(self.timezone))
            if self.check or self.settime:
                time_diff = round((station_time-current_time).total_seconds()/60)
                if self.settime and abs(time_diff) > self.timedelta:
                    new_time = datetime.now(pytz.timezone(self.timezone)).replace(tzinfo=None, microsecond=0)
                    self._process_set_time(new_time)
                    print("Setting new time to:", new_time)
                else:
                    print(time_diff)
            else:
                print(station_time)
        except Exception as e:
            self.logging_system.logger.error(str(e))
            self.logging_system.logger.debug(traceback.format_exc())
            raise click.ClickException(str(e))

    def _process_check_time(self):
        communicator = Communicator(self.portname, baud_rate=self.baudrate)
        communicator.open_port()
        davis_communicator = DavisCommunicator(communicator, self.logging_system.logger)
        davis_communicator.wake_up()
        davis_communicator.test_comm()
        dt = davis_communicator.get_time()
        communicator.close_port()
        return dt

    def _process_set_time(self, new_time):
        communicator = Communicator(self.portname, baud_rate=self.baudrate)
        communicator.open_port()
        davis_communicator = DavisCommunicator(communicator, self.logging_system.logger)
        davis_communicator.wake_up()
        davis_communicator.test_comm()
        davis_communicator.set_time(new_time)
        dt = davis_communicator.get_time()
        communicator.close_port()
        self.logging_system.logger.info(' Station new time: %s' % (dt.isoformat()))


class Logging:
    def __init__(self):
        self.logger = logging.getLogger("checktime")
        self.stdout_handler = logging.StreamHandler()
        self.logger.addHandler(self.stdout_handler)
        self.logger.setLevel("INFO")

    def log_start_of_execution(self):
        self.logger.info("Starting checktime, " + datetime.today().isoformat())

    def log_end_of_execution(self):
        self.logger.info("Finished checktime, " + datetime.today().isoformat())


if __name__ == "__main__":
    sys.exit(main())
