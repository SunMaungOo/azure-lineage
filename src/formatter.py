import logging
from typing import Set

class LogFormatter(logging.Formatter):

    ALLOWED_EXTRAS:Set[str] = {"event","pipeline","activity","dataset"}
    
    def format(self,record):
        message = super().format(record)

        extras = [
            f"{key}={value}"
            for key,value in record.__dict__.items()
            if key in self.ALLOWED_EXTRAS
        ]

        if len(extras)>0:
            extra_message = ' '.join(extras)

            message = f"{message} | {extra_message}"

        return message

