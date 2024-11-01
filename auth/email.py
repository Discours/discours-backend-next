import requests

from settings import MAILGUN_API_KEY, MAILGUN_DOMAIN

api_url = "https://api.mailgun.net/v3/%s/messages" % (MAILGUN_DOMAIN or "discours.io")
noreply = "discours.io <noreply@%s>" % (MAILGUN_DOMAIN or "discours.io")
lang_subject = {"ru": "Подтверждение почты", "en": "Confirm email"}


async def send_auth_email(user, token, lang="ru", template="email_confirmation"):
    try:
        to = "%s <%s>" % (user.name, user.email)
        if lang not in ["ru", "en"]:
            lang = "ru"
        subject = lang_subject.get(lang, lang_subject["en"])
        template = template + "_" + lang
        payload = {
            "from": noreply,
            "to": to,
            "subject": subject,
            "template": template,
            "h:X-Mailgun-Variables": '{ "token": "%s" }' % token,
        }
        print("[auth.email] payload: %r" % payload)
        # debug
        # print('http://localhost:3000/?modal=auth&mode=confirm-email&token=%s' % token)
        response = requests.post(api_url, auth=("api", MAILGUN_API_KEY), data=payload)
        response.raise_for_status()
    except Exception as e:
        print(e)
