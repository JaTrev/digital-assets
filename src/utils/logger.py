import logging
import sys

def get_production_logger(name: str):
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers if the logger is already setup
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Format for clean scannability in GitHub Actions logs
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Send to stdout so GitHub Actions can capture it
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger