import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from src.utils.logger import get_production_logger
from src.clients.db_client import SupabaseClient
from dotenv import load_dotenv

# Load local .env if present
load_dotenv()

logger = get_production_logger("email_job")

def send_email(df, recipients):
    sender = os.getenv("EMAIL_USER")
    password = os.getenv("GMAIL_APP_PASSWORD")
    
    msg = MIMEMultipart()
    # Updated Subject to indicate it's a Demo/Dummy run
    msg['Subject'] = f"Daily Crypto Snapshot"
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)

    # Convert DataFrame to HTML with basic styling
    html_table = df.to_html(index=False, border=1, justify='center')
    
    html_body = f"""
    <html>
        <head>
            <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.6; }}
            .banner {{ background-color: #fff3cd; border: 1px solid #ffeeba; color: #856404; padding: 15px; margin-bottom: 20px; border-radius: 4px; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
            th {{ background-color: #007bff; color: white; padding: 10px; text-align: left; }}
            td {{ padding: 8px; border-bottom: 1px solid #ddd; }}
            .footer {{ margin-top: 20px; font-size: 0.8em; color: #777; border-top: 1px solid #eee; padding-top: 10px; }}
            </style>
        </head>
        <body>
            <div class="banner">
            <strong>Project Update:</strong> This is an automated Dummy Job demonstrating our ability to fetch data from Supabase and deliver it via email.
            </div>
        
            <h3>Current Database Snapshot</h3>
            {html_table}
            
            <div class="footer">
                <p>Sent via <strong>GitHub Actions</strong> Pipeline.<br>
                Triggered by schedule: 08:00 UTC Daily.</p>
            </div>
        </body>
    </html>
    """
    msg.attach(MIMEText(html_body, 'html'))

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(sender, password)
        server.send_message(msg)

def main():
    logger.info("Starting email snapshot job...")
    try:
        db = SupabaseClient()
        
        query = "SELECT symbol, price, exchange, timestamp FROM exchange_prices ORDER BY timestamp DESC LIMIT 20"
        
        df = db.query(query) 

        if not df.empty:
            recipients = ["thies.jason@gmx.de"]
            send_email(df, recipients)
            logger.info(f"✅ Email sent to {len(recipients)} recipients.")
        else:
            logger.warning("⚠️ No data found to email. The table might be empty.")
            
    except Exception as e:
        logger.error(f"❌ Email job failed: {e}", exc_info=True)
        exit(1)

if __name__ == "__main__":
    main()