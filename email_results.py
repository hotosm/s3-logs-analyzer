import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTPAuthenticationError, SMTPException, SMTPServerDisconnected

import html2text

logging.basicConfig(level=logging.INFO)

smtp_server = os.getenv("SMTP_HOST")
port = int(os.getenv("SMTP_PORT", 587))
smtp_username = os.getenv("SMTP_USERNAME")
smtp_password = os.getenv("SMTP_PASSWORD")
from_email = os.getenv("FROM_EMAIL")
reply_to_email = os.getenv("REPLY_TO_EMAIL")


def send_email(to_emails, subject, content, content_type="plain"):
    msg = MIMEMultipart("alternative")
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = subject
    msg.add_header("Reply-To", reply_to_email)

    if content_type == "html":
        text_maker = html2text.HTML2Text()
        text_maker.ignore_links = False
        text_content = text_maker.handle(content)
        msg.attach(MIMEText(text_content, "plain"))
        msg.attach(MIMEText(content, "html"))
    else:
        msg.attach(MIMEText(content, "plain"))

    try:
        with smtplib.SMTP(smtp_server, port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
        logging.info("Email sent successfully!")
    except SMTPAuthenticationError:
        logging.error(
            "Failed to authenticate with SMTP server. Check your credentials."
        )
    except SMTPServerDisconnected:
        logging.error("Disconnected from SMTP server.")
    except SMTPException as e:
        logging.error(f"SMTP error occurred: {e}")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
