import logging
import sys
import os
from logging.handlers import TimedRotatingFileHandler

def setup_logger(name=__name__, log_dir="logs", log_file="billing_sync.log"):
    """
    Setup and configure logger with StreamHandler and TimedRotatingFileHandler.
    
    Args:
        name (str): Logger name.
        log_dir (str): Directory to store log files.
        log_file (str): Log filename.
        
    Returns:
        logging.Logger: Configured logger instance.
    """
    # Ensure logs directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_path = os.path.join(log_dir, log_file)
    
    # Check if logger already exists to avoid duplicate handlers
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger
        
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Console Handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # File Handler (Daily rotation, keep 30 days)
    file_handler = TimedRotatingFileHandler(
        log_path, when='midnight', interval=1, backupCount=30, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
