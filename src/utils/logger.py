import os
import logging
from logging.handlers import RotatingFileHandler

def get_logger(module_name: str, log_file: str = None, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(module_name)
    logger.setLevel(level)
    
    if logger.hasHandlers():
        logger.handlers.clear()
        
    formatter = logging.Formatter(
        '%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        # Giới hạn file 10MB, lưu tối đa 5 file cũ (tránh tràn ổ cứng)
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
    return logger