import pandas as pd
from datetime import datetime, timezone
from src.clients.db_client import SupabaseClient
from src.utils.logger import get_production_logger

logger = get_production_logger("dummy importer")

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