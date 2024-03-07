from datetime import date, datetime
import logging
import os
from time import strptime

from dotenv import load_dotenv
import pandas as pd
import sqlalchemy

def get_logger(log_level: str) -> logging.Logger:
        """
        Returns:
        - formatted logger
        """
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s: %(levelname)s: %(message)s'
        )
        logger = logging.getLogger()                    
        return logger