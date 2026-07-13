from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_alert_email(records_list, gmail_api_key):
    """Send email with high-value transaction alerts"""
    FROM_EMAIL = "tahafurkhan@gmail.com"
    TO_EMAIL = "tahafurkhan@gmail.com"
    
    # Build transaction table
    transaction_rows = ""
    for rec in records_list:
        transaction_rows += f"""
        <tr style="border-bottom: 1px solid #ddd;">
            <td style="padding: 10px;">{rec['alert_id']}</td>
            <td style="padding: 10px;">{rec['customer_name']}</td>
            <td style="padding: 10px;">{rec['customer_email']}</td>
            <td style="padding: 10px; font-weight: bold; color: #d32f2f;">{rec['currency']} {rec['transaction_amount']:,.2f}</td>
            <td style="padding: 10px;">{rec['currency']} {rec['transaction_limit']:,.2f}</td>
            <td style="padding: 10px;">{rec['merchant_name']}</td>
            <td style="padding: 10px;">{rec['city']}, {rec['country']}</td>
            <td style="padding: 10px;">{rec['transaction_timestamp']}</td>
        </tr>
        """
    
    body = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; color: #333; }}
            .header {{ background-color: #d32f2f; color: white; padding: 20px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th {{ background-color: #1976d2; color: white; padding: 10px; text-align: left; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>⚠️ High Value Transaction Alert</h2>
        </div>
        <div style="padding: 20px;">
            <p><strong>{len(records_list)} high-value transaction(s) detected</strong> exceeding customer limits.</p>
            <table border="1" style="border-color: #ddd;">
                <thead>
                    <tr>
                        <th>Alert ID</th>
                        <th>Customer</th>
                        <th>Email</th>
                        <th>Amount</th>
                        <th>Limit</th>
                        <th>Merchant</th>
                        <th>Location</th>
                        <th>Time</th>
                    </tr>
                </thead>
                <tbody>
                    {transaction_rows}
                </tbody>
            </table>
            <p><strong>Action Required:</strong> Review and contact customers if necessary.</p>
            <p>Thanks,<br><strong>FinGuard Alert System</strong></p>
        </div>
    </body>
    </html>
    """
    
    subject = f"⚠️ High Value Transaction Alert - {len(records_list)} Transaction(s)"
    
    msg = MIMEMultipart()
    msg["From"] = FROM_EMAIL
    msg["To"] = TO_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(FROM_EMAIL, gmail_api_key)
            server.send_message(msg)
        print(f"✅ Email sent successfully! {len(records_list)} alert(s) reported.")
    except Exception as e:
        print(f"❌ Error sending email: {e}")


@dp.foreach_batch_sink(name="email_alert_sink")
def process_alerts(df, batch_id):
    """Process each micro-batch and send email notifications"""
    records = df.collect()
    
    if len(records) > 0:
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(df.sparkSession)
            gmail_api_key = dbutils.secrets.get(scope="finguard-scope", key="gmail_api_key")
        except Exception as e:
            print(f"⚠️ Could not retrieve Gmail API key: {e}")
            return
        
        # Convert to simple dictionaries
        records_list = []
        for row in records:
            records_list.append({
                'alert_id': row.alert_id,
                'customer_name': row.customer_name,
                'customer_email': row.customer_email,
                'transaction_amount': float(row.transaction_amount),
                'transaction_limit': float(row.transaction_limit),
                'currency': row.currency,
                'merchant_name': row.merchant_name,
                'city': row.city,
                'country': row.country,
                'transaction_timestamp': str(row.transaction_timestamp)
            })
        
        send_alert_email(records_list, gmail_api_key)
    else:
        print("ℹ️ No high-value transactions in this batch.")


@dp.append_flow(target="email_alert_sink")
def stream_alerts_to_email() -> DataFrame:
    """Stream high-value alerts to email notification sink"""
    return spark.readStream.table("finguard.gold.high_value_transactions_alert")