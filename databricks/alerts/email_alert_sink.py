import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pyspark import pipelines as dp

# Gmail account credentials
EMAIL = "joshp3781@gmail.com"
APP_PASSWORD = dbutils.secrets.get(scope="Financial-guard-scope", key="gmail_app_password")


@dp.foreach_batch_sink(name="email_alert_sink")
def email_alert_sink(df, batch_id):
    rows = df.collect()
    for row in rows:
        to_email = row["customer_email"]
        if to_email is None:
            continue

        subject = f"⚠️ High Value Transaction Alert - {row['transaction_id']}"
        body = f"""
<html><body style="margin:0;padding:0;background-color:#f4f4f4;font-family:Arial,Helvetica,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f4;padding:20px 10px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff;border-radius:10px;overflow:hidden;border:1px solid #dddddd;">
        <tr>
          <td style="background-color:#e67e22;padding:22px 30px;text-align:center;">
            <h1 style="color:#ffffff;margin:0;font-size:22px;">⚠️ High Value Transaction Alert</h1>
            <p style="color:#fdebd0;margin:8px 0 0 0;font-size:13px;">A transaction above your configured limit was detected on your account</p>
          </td>
        </tr>
        <tr>
          <td style="padding:16px 30px 0 30px;">
            <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
              <tr>
                <td align="center" style="padding:14px;background-color:#fef5e7;border:1px solid #e67e22;border-radius:6px;">
                  <span style="color:#935116;font-size:12px;letter-spacing:1px;">TRANSACTION AMOUNT</span><br>
                  <span style="color:#e67e22;font-size:26px;font-weight:bold;">{row['transaction_amount']} {row['currency']}</span><br>
                  <span style="color:#935116;font-size:12px;">Your configured limit: {row['transaction_limit']}</span>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        <tr>
          <td style="padding:16px 30px 0 30px;">
            <p style="font-size:14px;color:#333333;line-height:1.6;">Dear <b>{row['customer_name']}</b>,</p>
            <p style="font-size:13px;color:#555555;line-height:1.6;">The following transaction exceeded the limit configured on your account. Please review it and confirm whether you authorized it.</p>
          </td>
        </tr>
        <tr>
          <td style="padding:10px 30px;">
            <h3 style="color:#e67e22;font-size:15px;border-bottom:2px solid #e67e22;padding-bottom:5px;margin:16px 0 10px 0;">💰 Transaction Details</h3>
            <table width="100%" cellpadding="8" cellspacing="0" style="border-collapse:collapse;font-size:13px;color:#333333;">
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;width:40%;font-weight:bold;">Alert ID</td><td style="border:1px solid #e0e0e0;">{row['alert_id']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Transaction ID</td><td style="border:1px solid #e0e0e0;">{row['transaction_id']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Transaction Type</td><td style="border:1px solid #e0e0e0;">{row['transaction_type']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Payment Channel</td><td style="border:1px solid #e0e0e0;">{row['payment_channel']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Transaction Timestamp</td><td style="border:1px solid #e0e0e0;">{row['transaction_timestamp']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Status</td><td style="border:1px solid #e0e0e0;">{row['status']}</td></tr>
            </table>
            <h3 style="color:#e67e22;font-size:15px;border-bottom:2px solid #e67e22;padding-bottom:5px;margin:20px 0 10px 0;">🏪 Merchant &amp; Location</h3>
            <table width="100%" cellpadding="8" cellspacing="0" style="border-collapse:collapse;font-size:13px;color:#333333;">
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;width:40%;font-weight:bold;">Merchant</td><td style="border:1px solid #e0e0e0;">{row['merchant_name']} ({row['merchant_category']})</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Location</td><td style="border:1px solid #e0e0e0;">{row['city']}, {row['country']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">International</td><td style="border:1px solid #e0e0e0;">{'Yes' if row['is_international'] else 'No'}</td></tr>
            </table>
          </td>
        </tr>
        <tr>
          <td style="background-color:#f9f9f9;padding:16px 30px;border-top:1px solid #eeeeee;text-align:center;">
            <p style="font-size:12px;color:#999999;margin:0;">This is an automated alert from the Financial Guard monitoring system.<br>If you did not authorize this transaction, please contact support immediately.</p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body></html>
"""

        msg = MIMEMultipart()
        msg["From"] = EMAIL
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))

        try:
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(EMAIL, APP_PASSWORD)
                server.send_message(msg)
        except Exception as e:
            print(f"❌ Error sending email to {to_email}: {e}")


@dp.append_flow(target="email_alert_sink", name="alert_email_flow")
def alert_email_flow():
    return spark.readStream.table("financial_guard.gold.high_value_transaction_alert")