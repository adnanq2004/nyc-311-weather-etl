# Importing libraries for a reusable logging function
import logging 
from pathlib import Path


# Creating a reusable class in order to be able to log each part of the ETL process
class ETLLogger:
    def __init__(self, name, log_folder = "../logs"):
        self.log_folder = Path(log_folder)
        self.log_folder.mkdir(parents = True, exist_ok = True)
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            file_handler = logging.FileHandler(self.log_folder / f"{name}.log")
            formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def get(self):
        return self.logger
