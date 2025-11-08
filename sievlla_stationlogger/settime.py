import logging
import sys
import traceback

import click

from datetime import datetime
from sievlla_stationlogger.communication.serial_comm import Communicator
from sievlla_stationlogger.communication.davis import DavisCommunicator

@click.command()
@click.option(
    "--baudrate", is_flag=False, default="19200", help="Baudrate",
)
@click.argument("portname")
@click.argument("newtime")
@click.version_option( prog_name="checktime")
def main(baudrate, portname, newtime):
    """Insert meteorological logger data to Enhydris"""
    SetTime(baudrate, portname,newtime).run()


class SetTime:
    def __init__(self, baudrate, portname, newtime):
        self.baudrate = int(baudrate)
        self.portname = portname
        self.newtime = datetime.strptime(newtime, "%Y-%m-%dT%H:%M:%S")
        self.logging_system = Logging()

    def run(self):
        try:
            self.logging_system.log_start_of_execution()
            self._process_set_time()
            self.logging_system.log_end_of_execution()
        except Exception as e:
            self.logging_system.logger.error(str(e))
            self.logging_system.logger.debug(traceback.format_exc())
            raise click.ClickException(str(e))

    def _process_set_time(self):
        communicator = Communicator(self.portname, baud_rate=self.baudrate)
        communicator.open_port()
        davis_communicator = DavisCommunicator(communicator, self.logging_system.logger)
        davis_communicator.wake_up()
        davis_communicator.test_comm()
        davis_communicator.set_time(self.newtime)
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