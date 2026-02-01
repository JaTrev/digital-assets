import logging
import pandas as pd
from datetime import datetime, timezone
from src.clients.db_client import SupabaseClient

# 1. Setup Production Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("heartbeat_job")

def main():
    logger.info("Starting dummy heartbeat job...")
    
    try:
        db = SupabaseClient()
        
        # Create dummy data
        data = pd.DataFrame([{
            "symbol": "DUMMY",
            "price": 1.0,
            "exchange": "internal",
            "timestamp": datetime.now(timezone.utc)
        }])
        
        logger.info(f"Attempting to write heartbeat row to database...")
        db.write_df(data, "exchange_prices")
        logger.info("✅ Heartbeat successfully recorded.")
        
    except Exception as e:
        logger.error(f"❌ Job failed: {str(e)}", exc_info=True)
        exit(1) # Signal failure to Cloud Run

if __name__ == "__main__":
    main()