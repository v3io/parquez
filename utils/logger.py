# Copyright 2019 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import logging
from colorlog import ColoredFormatter

LOG_FILE_NAME = "parquez.log"


class Logger:
    def __init__(self, level=logging.INFO):
        self.level = level
        self.log_filename = LOG_FILE_NAME
        self.logger = self.init_logger()

    def init_logger(self):
        logging.basicConfig(filename=self.log_filename, level=self.level,
                            format='%(asctime)s %(threadName)s	 %(module)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p')

        logger = logging.Logger(__name__,  self.level)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        color_formatter = ColoredFormatter(
            "%(log_color)s %(asctime)s - %(name)s - %(levelname)s - %(message)s%(reset)s")
        sh = logging.StreamHandler()
        sh.setLevel(self.level)
        sh.setFormatter(color_formatter)
        handler = logging.FileHandler(self.log_filename, mode='w')
        handler.setLevel(self.level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
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


