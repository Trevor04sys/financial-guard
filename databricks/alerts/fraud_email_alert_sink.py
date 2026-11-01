import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pyspark import pipelines as dp

# Gmail account credentials
EMAIL = "joshp3781@gmail.com"
APP_PASSWORD = dbutils.secrets.get(scope="Financial-guard-scope", key="gmail_app_password")

@dp.foreach_batch_sink(name="fraud_email_alert_sink")
def fraud_email_alert_sink(df, batch_id):
    rows = df.collect()
    for row in rows:
        to_email = row["customer_email"]
        if to_email is None:
            continue

        card_number = row["card_number"] or ""
        masked_card = f"****-****-****-{card_number[-4:]}" if len(card_number) >= 4 else "****"

        risk_color = {"high": "#c0392b", "medium": "#e67e22", "low": "#f39c12"}.get(str(row["risk_level"]).lower(), "#7f8c8d")

        subject = f"🚨 Fraud Alert ({row['risk_level']}) - {row['transaction_id']}"
        body = f"""
<html><body style="margin:0;padding:0;background-color:#f4f4f4;font-family:Arial,Helvetica,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f4;padding:20px 10px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff;border-radius:10px;overflow:hidden;border:1px solid #dddddd;">
        <tr>
          <td style="background-color:#c0392b;padding:22px 30px;text-align:center;">
            <h1 style="color:#ffffff;margin:0;font-size:22px;">🚨 Fraud Watchlist Match Alert</h1>
            <p style="color:#fadbd8;margin:8px 0 0 0;font-size:13px;">A transaction on your card matched a known fraud watchlist entry</p>
          </td>
        </tr>
        <tr>
          <td style="padding:16px 30px 0 30px;">
            <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
              <tr>
                <td align="center" style="padding:14px;background-color:{risk_color};border-radius:6px;">
                  <span style="color:#ffffff;font-size:14px;font-weight:bold;letter-spacing:1px;">RISK LEVEL: {row['risk_level']} &nbsp;|&nbsp; ACTION: {row['action']}</span><br>
                  <span style="color:#ffffff;font-size:12px;">{row['reason_code']} — {row['reason_description']}</span>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        <tr>
          <td style="padding:16px 30px 0 30px;">
            <p style="font-size:14px;color:#333333;line-height:1.6;">Dear <b>{row['customer_name']}</b>,</p>
            <p style="font-size:13px;color:#555555;line-height:1.6;">Our fraud detection system flagged the transaction below because your card <b>{masked_card}</b> matches an entry on the fraud watchlist. Please review it immediately and contact support if you did not authorize it.</p>
          </td>
        </tr>
        <tr>
          <td style="padding:10px 30px;">
            <h3 style="color:#c0392b;font-size:15px;border-bottom:2px solid #c0392b;padding-bottom:5px;margin:16px 0 10px 0;">🧾 Transaction Details</h3>
            <table width="100%" cellpadding="8" cellspacing="0" style="border-collapse:collapse;font-size:13px;color:#333333;">
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;width:40%;font-weight:bold;">Transaction ID</td><td style="border:1px solid #e0e0e0;">{row['transaction_id']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fdedec;font-weight:bold;">Amount</td><td style="border:1px solid #e0e0e0;background-color:#fdedec;color:#c0392b;font-weight:bold;">{row['amount']} {row['currency']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Card Number</td><td style="border:1px solid #e0e0e0;">{masked_card}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Transaction Type</td><td style="border:1px solid #e0e0e0;">{row['transaction_type']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Payment Channel</td><td style="border:1px solid #e0e0e0;">{row['payment_channel']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Merchant</td><td style="border:1px solid #e0e0e0;">{row['merchant_name']} ({row['merchant_category']})</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Device</td><td style="border:1px solid #e0e0e0;">{row['device_id']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Location</td><td style="border:1px solid #e0e0e0;">{row['transaction_city']}, {row['transaction_country']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">International</td><td style="border:1px solid #e0e0e0;">{'Yes' if row['is_international'] else 'No'}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Transaction Timestamp</td><td style="border:1px solid #e0e0e0;">{row['transaction_timestamp']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Status</td><td style="border:1px solid #e0e0e0;">{row['transaction_status']}</td></tr>
            </table>
            <h3 style="color:#c0392b;font-size:15px;border-bottom:2px solid #c0392b;padding-bottom:5px;margin:20px 0 10px 0;">📋 Watchlist Details</h3>
            <table width="100%" cellpadding="8" cellspacing="0" style="border-collapse:collapse;font-size:13px;color:#333333;">
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;width:40%;font-weight:bold;">Alert ID</td><td style="border:1px solid #e0e0e0;">{row['alert_id']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Watchlist ID</td><td style="border:1px solid #e0e0e0;">{row['watchlist_id']} ({row['watch_type']})</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Reported By</td><td style="border:1px solid #e0e0e0;">{row['reported_by']} ({row['reported_source']})</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Card on Watchlist Since</td><td style="border:1px solid #e0e0e0;">{row['watchlist_effective_from']}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Watchlist Location</td><td style="border:1px solid #e0e0e0;">{row['watchlist_city']}, {row['watchlist_country']}</td></tr>
            </table>
            <h3 style="color:#c0392b;font-size:15px;border-bottom:2px solid #c0392b;padding-bottom:5px;margin:20px 0 10px 0;">🔍 Risk Assessment</h3>
            <table width="100%" cellpadding="8" cellspacing="0" style="border-collapse:collapse;font-size:13px;color:#333333;">
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;width:40%;font-weight:bold;">High Risk</td><td style="border:1px solid #e0e0e0;">{'🔴 YES' if row['is_high_risk'] else 'No'}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Location Mismatch vs Watchlist</td><td style="border:1px solid #e0e0e0;">Country: {'Yes' if row['geo_country_mismatch'] else 'No'}, City: {'Yes' if row['geo_city_mismatch'] else 'No'}</td></tr>
              <tr><td style="border:1px solid #e0e0e0;background-color:#fafafa;font-weight:bold;">Days Since Card Placed on Watchlist</td><td style="border:1px solid #e0e0e0;">{row['days_since_watchlist_entry']}</td></tr>
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


@dp.append_flow(target="fraud_email_alert_sink", name="fraud_alert_email_flow")
def fraud_alert_email_flow():
    return spark.readStream.table("financial_guard.gold.fraud_card_alert")