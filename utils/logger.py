import os
import logging
from colorlog import ColoredFormatter


DEBUG_LEVEL = logging.INFO

class Logger:
    def __init__(self):
        self.log_filename = __name__+".log"
        self.logger = self.init_logger()

    def init_logger(self):
        logging.basicConfig(filename=self.log_filename, level=DEBUG_LEVEL,
                            format='%(asctime)s %(threadName)s	 %(module)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p')

        logger = logging.Logger(__name__, DEBUG_LEVEL)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        color_formatter = ColoredFormatter(
            "%(log_color)s %(asctime)s - %(name)s - %(levelname)s - %(message)s%(reset)s")
        sh = logging.StreamHandler()
        sh.setLevel(DEBUG_LEVEL)
        sh.setFormatter(color_formatter)
        hdlr = logging.FileHandler(self.log_filename, mode='w')
        hdlr.setLevel(DEBUG_LEVEL)
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.addHandler(sh)
        logger.info("Logger Initialized")
        logger.info('Log file location: {}'.format(os.path.abspath(self.log_filename)))
        return logger

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)
