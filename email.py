import os
import smtplib

smtp_server = os.getenv("SMTP_SERVER")
port = int(os.getenv("SMTP_PORT", 587))
smtp_username = os.getenv("SMTP_USERNAME")
smtp_password = os.getenv("SMTP_PASSWORD")
from_email = os.getenv("FROM_EMAIL")
reply_to_email = os.getenv("REPLY_TO_EMAIL")


def send_email(to_emails, subject, text_content):
    headers = [
        f"From: {from_email}",
        f"To: {', '.join(to_emails)}",
        f"Subject: {subject}",
        f"Reply-To: {reply_to_email}",
        "Content-Type: text/plain; charset=utf-8",
    ]
    headers = "\r\n".join(headers)

    message = headers + "\r\n\r\n" + text_content

    try:
        server = smtplib.SMTP(smtp_server, port)
        server.starttls()  # Secure the connection
        server.login(smtp_username, smtp_password)
        server.sendmail(from_email, to_emails, message)
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")


# Example usage
# subject = "Your Subject Here"
# to_emails = ["recipient@example.com", "another@example.com"]
# text_content = "This is the body of the email."

# send_email(to_emails, subject, text_content)
