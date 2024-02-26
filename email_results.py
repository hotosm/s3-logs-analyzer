import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTPAuthenticationError, SMTPException, SMTPServerDisconnected

logging.basicConfig(level=logging.INFO)

smtp_server = os.getenv("SMTP_HOST")
port = int(os.getenv("SMTP_PORT", 587))
smtp_username = os.getenv("SMTP_USERNAME")
smtp_password = os.getenv("SMTP_PASSWORD")
from_email = os.getenv("FROM_EMAIL")
reply_to_email = os.getenv("REPLY_TO_EMAIL")


def send_email(to_emails, subject, content, content_type="plain"):

    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = subject
    msg.add_header("Reply-To", reply_to_email)

    msg.attach(MIMEText(content, content_type))

    try:
        with smtplib.SMTP(smtp_server, port) as server:
            server.starttls()  # Secure the connection
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


# Example usage
# subject = "Your Subject Here"
# to_emails = ["recipient@example.com", "another@example.com"]
# text_content = "This is the body of the email."
# send_email(to_emails, subject, text_content, 'plain')
