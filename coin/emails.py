
from algorithm import run

COIN_EMAIL = 'coin.project.huji@gmail.com'

import smtplib

from email.mime.text import MIMEText


def sendEmail(to, content):
    # sendFirstEmail(to, content)
    sendSecondEmail(to, content)


def sendFirstEmail(to, content):
    msg = MIMEText("We are searching results for your search: " + content + "\n"
                   + "An email with our algorithm's results will be send to your email address : " + to + " soon.")
    msg['Subject'] = "Search request submitted"
    msg['From'] = "coin.project@sandboxcf29099671c7490eb8f0475d59ba01a7.mailgun.org"
    msg['To'] = to

    s = smtplib.SMTP('smtp.mailgun.org', 587)
    # Todo - change login settings for your new user..
    # s.login('postmaster@sandboxcf29099671c7490eb8f0475d59ba01a7.mailgun.org',
    #         '06622845eb4a6500758ff47bf996cf19-060550c6-e56db87b')
    s.sendmail(msg['From'], msg['To'], msg.as_string())
    s.quit()


def sendSecondEmail(to, content):
    try:
        algorithm_results = run(content)
    except:
        print ("sending email to: " + to + " with content: " + content + " failed")
        return
    msg = MIMEText("And the top results are:\n" + algorithm_results)
    msg['Subject'] = "Search request results"
    msg['From'] = "coin.project@sandboxcf29099671c7490eb8f0475d59ba01a7.mailgun.org"
    msg['To'] = to

    s = smtplib.SMTP('smtp.mailgun.org', 587)
    # Todo - change login settings for your new user..
    # s.login('postmaster@sandboxcf29099671c7490eb8f0475d59ba01a7.mailgun.org',
    #         '06622845eb4a6500758ff47bf996cf19-060550c6-e56db87b')
    s.sendmail(msg['From'], msg['To'], msg.as_string())
    s.quit()
