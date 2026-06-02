import sys
from types import ModuleType

from .logger import get_logger

_logger = get_logger(__name__)


class LichessException(Exception):
    def __init__(self, error_message, error_details: ModuleType):
        self.error_message = error_message
        _, _, exc_tb = error_details.exc_info()

        if exc_tb is not None:
            self.lineno = exc_tb.tb_lineno
            self.file_name = exc_tb.tb_frame.f_code.co_filename
        else:
            self.lineno = "unknown"
            self.file_name = "unknown"

        _logger.error("%s", str(error_message), exc_info=True)

    def __str__(self):
        return (
            "Error occurred in python script name [{0}] line number [{1}] error message [{2}]"
        ).format(self.file_name, self.lineno, str(self.error_message))


if __name__ == "__main__":
    try:
        _logger.info("Enter the try block")
        a = 1 / 0
        print("This will not be printed", a)
    except Exception as e:
        raise LichessException(e, sys) from e

