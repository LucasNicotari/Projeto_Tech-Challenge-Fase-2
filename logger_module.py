import logging
import sys
from datetime import datetime
from pathlib import Path
from config_module import Config

class Logger:
    def __init__(self, name: str, level: str = 'INFO'):
        self.name = name
        self.level = getattr(logging, level.upper())
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)
        
        if logger.handlers:
            logger.handlers.clear()
            
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        Config.create_directories()
        log_file = Config.LOGS_DIR / f"{self.name}_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(self.level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    def info(self, message: str):
        self.logger.info(message)
    
    def error(self, message: str):
        self.logger.error(message)
    
    def warning(self, message: str):
        self.logger.warning(message)
    
    def debug(self, message: str):
        self.logger.debug(message)
    
    def success(self, message: str):
        self.logger.info(message)
    
    def process(self, message: str):
        self.logger.info(message)
    
    def failure(self, message: str):
        self.logger.error(message)

def get_logger(name: str, level: str = 'INFO') -> Logger:
    return Logger(name, level)
