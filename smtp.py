import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import requests

import logging

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

SMTP_INFO = {
    "smtp_server": os.getenv('SMTP_SERVER', "smtp.serviciodecorreo.es"),
    "smtp_port": os.getenv('SMTP_PORT', 465),
    "smtp_user": os.getenv('SMTP_USER', "alert@elliotcloud.com"),
    "smtp_pass": os.getenv('SMTP_PASS', "password")
}


def smtp_send(message: str, email_subject: str, receivers: list):
    # COMPOSE EMAIL
    msg = MIMEMultipart()
    msg['From'] = "alert@elliotcloud.com"
    msg['To'] = "; ".join(receivers)
    msg['Subject'] = email_subject
    msg.attach(MIMEText(message, 'plain'))
    try:
        logging.info(f"Trying to connect to the SMTP server...")
        smtpObj = smtplib.SMTP_SSL(SMTP_INFO['smtp_server'], SMTP_INFO['smtp_port'])
        smtpObj.set_debuglevel(False)
        smtpObj.login(SMTP_INFO['smtp_user'], SMTP_INFO['smtp_pass'])
        smtpObj.sendmail(msg['From'], receivers, msg.as_string())
    except (smtplib.SMTPException, TimeoutError) as error:
        logging.exception(f"Error SMPT {error}")
    finally:
        if smtpObj:
            smtpObj.quit()
            logging.info("SMTP server connection closed")


def bot_send_text(bot_message: str, apikey: list, chatid: list):
    for i in range(len(apikey)):
        bot_token = apikey[i]
        bot_chatID = chatid[i]
        send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=HTML&text=' + bot_message
        response = requests.get(send_text)
        logging.info(response)
    return 0
