import logging
from typing import Optional, Union

class Color:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    BRIGHTGREEN = '\u001b[32;1m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

class Logger:

    @staticmethod
    def get_logger(module: str, logging_level: Optional[Union[int, str]] = logging.DEBUG) -> logging.Logger:

        logging.basicConfig(format=f'{Color.BOLD}[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d]{Color.END}'
                                f' %(message)s')  # do not set level here as it will affect py4j
        logger = logging.getLogger(module)
        logger.setLevel(logging_level)
        return logger