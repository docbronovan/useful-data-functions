
def set_mailgun_api():
    api_key = 'ab26471d4a88fa24a409c319fe335d47'
    domain = 'sandboxdf942f381ce24f8a8e8e07a1e68e3c65.mailgun.org'
    return api_key, domain


def set_postgres_params():
    postgres_host = 'mlsanalytics.cstfzice6ibf.us-east-1.rds.amazonaws.com'
    postgres_user = 'root'
    postgres_pw = 'yT8Kdoyj'
    postgres_db = "mlsanalytics"
    return postgres_host, postgres_user, postgres_pw, postgres_db
