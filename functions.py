################
# # STATS # #
################

import numpy as np


def is_outlier(points, thresh=3.5):
    """
    Stack overflow answer from Joe Kington
    https://stackoverflow.com/questions/22354094/
        pythonic-way-of-detecting-outliers-in-one-dimensional-observation-data

    Returns a boolean array with True if points are outliers and False
    otherwise. thresh is the number of standard deviations from the mean you consider and outlier

    Parameters:
    -----------
        points : An numobservations by numdimensions array of observations
        thresh : The modified z-score to use as a threshold. Observations with
            a modified z-score (based on the median absolute deviation) greater
            than this value will be classified as outliers.

    Returns:
    --------
        mask : A numobservations-length boolean array.

    References:
    ----------
        Boris Iglewicz and David Hoaglin (1993), "Volume 16: How to Detect and
        Handle Outliers", The ASQC Basic References in Quality Control:
        Statistical Techniques, Edward F. Mykytka, Ph.D., Editor.
    https://stackoverflow.com/questions/22354094/pythonic-way-of-detecting-outliers-in-one-dimensional-observation-data
    """
    if len(points.shape) == 1:
        points = points[:, None]
    median = np.median(points, axis=0)
    diff = np.sum((points - median)**2, axis=-1)
    diff = np.sqrt(diff)
    med_abs_deviation = np.median(diff)

    modified_z_score = 0.6745 * diff / med_abs_deviation

    return modified_z_score > thresh


def outlier_score(points):
    """
    Stack overflow answer from Joe Kington
    https://stackoverflow.com/questions/22354094/
        pythonic-way-of-detecting-outliers-in-one-dimensional-observation-data

    Returns a boolean array with True if points are outliers and False
    otherwise.

    Parameters:
    -----------
        points : An numobservations by numdimensions array of observations
        thresh : The modified z-score to use as a threshold. Observations with
            a modified z-score (based on the median absolute deviation) greater
            than this value will be classified as outliers.

    Returns:
    --------
        mask : A numobservations-length numeric array.

    References:
    ----------
        Boris Iglewicz and David Hoaglin (1993), "Volume 16: How to Detect and
        Handle Outliers", The ASQC Basic References in Quality Control:
        Statistical Techniques, Edward F. Mykytka, Ph.D., Editor.
    """
    if len(points.shape) == 1:
        points = points[:, None]
    median = np.median(points, axis=0)
    diff = np.sum((points - median)**2, axis=-1)
    diff = np.sqrt(diff)
    med_abs_deviation = np.median(diff)

    modified_z_score = 0.6745 * diff / med_abs_deviation

    return modified_z_score


""" 
    to check for outliers in numeric column of a dataframe. You may set your own threshold.
    is_outlier() accepts an array, list, or series returns true if outlier
    
        import pandas as pd
        df = pd.DataFrame({"values":[1,2,2,2,2,3,77,89]})
        points = df['values']
        data = is_outlier(points, thresh=3.5)
        avg = df[~data]['values'].mean()
        # avg = 2
"""


######################
# # # POSTGRESQL # # #
######################
import psycopg2
import pandas as pd
from my_secrets import set_postgres_params


def set_cursor():
    """ relies on set_postgres_params which sets sensative parameters """
    postgres_host, postgres_user, postgres_pw, postgres_db = set_postgres_params()
    conn = psycopg2.connect(host=postgres_host, database=postgres_db, user=postgres_user, password=postgres_pw)
    cur = conn.cursor()
    return conn, cur


def check_and_insert_postgres(cur, index_col, table_name, df_to_insert):
    """
    Gets key value from table's index columns to check against. If not matched then we need to make a
    new row in the table.
    :param cur: cursor object
    :param index_col: str - col to check whether to insert
    :param table_name: str - table you want to check
    :param df_to_insert: dataframe with values you want to check if already inserted
    :return:
    """
    sql = 'select {} from {};'.format(index_col, table_name)
    column_names_str = ','.join(list(df_to_insert.columns.values))
    already_written = return_sql(sql, cur, column_names_str)
    for value in df_to_insert[index_col]:
        if value not in already_written:
            row_values = df_to_insert.loc[df_to_insert[index_col] == value].iloc[0].values
            row_values_str = '\',\''.join([str(i) for i in row_values])
            sql = """INSERT INTO {} ({}) VALUES ('{}');""".format(table_name, column_names_str, row_values_str)
            try:
                run_sql(sql, cur)
            except (RuntimeError, TypeError, NameError):
                print('error {}'.format(str((RuntimeError, TypeError, NameError))))
    return


def return_sql(sql, cur, columns):
    """
    Returns dataframe result of sql query
    :param cur: sql db cursor
    :param sql: sql to run
    :param columns: list of column names
    :return: dataframe
    """
    cur.execute(sql)
    data = cur.fetchall()  # returns tuple
    df = pd.DataFrame(data)
    if not pd.DataFrame(data).empty:
        df.columns = columns
    return df


def run_sql(sql, cur):
    """
    :param cur: sql db cursor
    :param sql: str - sql to run
    :return: list
    """
    cur.execute("BEGIN;")
    cur.execute(sql)
    cur.execute("COMMIT;")
    return


#####################
# # GOOGLE SHEETS # #
#####################
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def setup_google_creds():
    """ use creds to create a client to interact with the Google Drive API. 
        Code based on Twilio blog post:
        https://www.twilio.com/blog/2017/02/an-easy-way-to-read-and-write-to-a-google-spreadsheet-in-python.html
        :return: google client object 
    """
    print('setup google client')
    scope = ['https://spreadsheets.google.com/feeds']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)
    return client


def get_spreadsheet_values(client, gsheet_name):
    """
    get values from google spreadsheet
    :param client: google sheet object
    :param gsheet_name: string, name of google sheet to retrieve data from
    :return: dataframe
    """
    print('getting spreadsheet values')
    sheet = client.open(gsheet_name).sheet1
    # Find a workbook by name and open the first sheet. 
    # Gspread only allows you to open a sheet named sheet1
    try:
        list_of_hashes = sheet.get_all_records()
    except AttributeError:
        print('get_spreadsheet_values: AttributeError')
        list_of_hashes = []
    sheet_values_df = pd.DataFrame(list_of_hashes)
    return sheet_values_df


########################
# # GOOGLE ANALYTICS # #
########################
# https://developers.google.com/api-client-library/python/start/get_started #
import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build


def build_service_url():
    print('build service url')
    json_file = 'GA_Account_Name_75c59894dab2.json'  # your service account private key json file
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    json_file,
                    ['https://www.googleapis.com/auth/analytics.readonly'])
    # create a service object you'll later on to create reports
    http = credentials.authorize(httplib2.Http())
    service = build('analytics', 'v4', http=http,
                    discoveryServiceUrl=('https://analyticsreporting.googleapis.com/$discovery/rest'))
    return service


def create_query_params(view_id, start_date, end_date):
    """ This query returns total users between two dates.
        You can find query parameter documentation here:
        https://developers.google.com/analytics/devguides/reporting/core/v4/basics#metrics  
        :param view_id: string
        :param start_date: datetime object
        :param end_date: datetime object
        :return: dictionary
    """
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    print("|| ", start_date_str, " - ", end_date_str)
    query_params = {'viewId': view_id,
                    'dateRanges': [{'startDate': start_date_str, 'endDate': end_date_str}],
                    'metrics': [{'expression': 'ga:users'}],
                    'pageSize': 10
                    }
    return query_params


def get_response(service, query_params):
    """ :param service: google service obj
        :param query_params: dictionary
        :return: dictionary
        Can check view Ids at: https://ga-dev-tools.appspot.com/account-explorer/
          by selecting the account and view you want to query.
    """
    print('get response')
    response = service.reports().batchGet(
                    body={
                        'reportRequests': [query_params]
                    }
                ).execute()
    return response


def parse_response(response):
    """ 
    :param response: dictionary
    :return: integer
    The response back from the api is in the format below. The column names and types
    are seperate from the data and metadata.
    response.keys() >> [u'reports']
    type(response['reports']) >> list
    len(response['reports']) >> 1
    Format of response['reports']
    {u'columnheader':
        {u'metricHeader': 
            {u'metricHeaderEntries': 
                [{u'name': u'ga:users',
                  u'type': u'INTEGER'}]
            }
        },
        u'data': 
            {u'maximums': 
                [{u'values': [u'999']}],
             u'minimums': [{u'values': [u'999']}],
             u'rowCount': 1,
             u'rows': 
                [{u'metrics': 
                    [{u'values': [u'999']}]
                }],
             u'totals': 
                [{u'values': [u'999']}]
            }
    }
    """
    val = []
    for report in response.get('reports', []):
        columnheader = report.get('columnheader', {})
        metricheaders = columnheader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])
        for row in rows:
            daterangevalues = row.get('metrics', [])
            for i, values in enumerate(daterangevalues):
                for metricheader, value in zip(metricheaders, values.get('values')):
                    val.append(int(value))
    return val


"""
    EXAMPLE USAGE
    service = build_service_url()
    view_id = '999999999'  # your viewId string
    d = 1  # set number of days to go back
    end_date = datetime.datetime.today() - timedelta(days=d)
    start_date = datetime.date(end_date.year, 1, 1)
    query_params = create_query_params(view_id, start_date, end_date)
    response = get_response(service, query_params)
    val = parse_response(response)
"""


######################################
# # # SEND EMAIL VIA MAILGUN API # # #
######################################
"""
To use send_error_email(), use a try/except around main
if __name__ == "__main__":
    errors_to_ignore = ['502', '500']
    try:
        main()
        print "Script Finished Successfully"
    except Exception, e:
        send_error_email(e, errors_to_ignore)

To send a dataframe as a html table:
    import datetime
    dateobj = datetime.datetime.today()
    df_two = pd.DataFrame({"names":['first','second','third'],
                       "values":[1,2,89]})
    subject = "Test email"
    report_name = "Test Report"
    html = create_table_header(dateobj, report_name)
    html = create_html_table(df_two, html)
    html += "<br><br><br><br>"
    send_html_mailgun(subject, html, ['youremail@gmail.com'])
"""
import subprocess
import sys
import os
from my_secrets import set_mailgun_api


def send_error_email(e, errors_to_ignore, recipients=['youremail@gmail.com']):
    """
    This is meant to be used as an email alert when a script fails for whatever reason
    :param e:
    :param errors_to_ignore:
    :param recipients:
    :return:
    """
    send = check_errors(e, errors_to_ignore)
    if send:
        # subject = "Error"
        subject = sys.argv[0].split('/')[-1]  # this is the name of the python script being executed
        body = str(e)
        send_html_mailgun(subject, body, recipients)
    return


def check_errors(e, errors_to_ignore):
    """
    This function was created to allow you to ignore an error if you would like, if the error is included in the
    'errors to ignore' list, don't send an email
    :param e: error
    :param errors_to_ignore: list
    """
    print("ERROR: {}".format(e))
    # if the error is in any of the errors_to_ignore list, don't send
    send = 1
    for err in errors_to_ignore:
        if err in str(e):
            send = 0
            break
        else:
            send = 1
    return send


def send_html_mailgun(subject, body, recipients=['youremail@gmail.com']):
    """
    calls create function which returns a bash command. Use subprocess to run this bash command, then
    remove html file created in create_mailgun_html
    :param subject:
    :param body:
    :param recipients:
    :return: None
    """
    bash_command, local_temp_path = create_mailgun_html(subject, body, recipients)
    subprocess.call(bash_command, shell=True)
    os.remove(local_temp_path)
    return


def create_mailgun_html(subject, html, recipients=['youremail@gmail.com']):
    """
    Retrieve mailgun parameters from my_secrets file - contains api key and sending domain.
    Creates bash command run by send_html_mailgun(). Creates local html file (mailgun_html.html),
    then calls create_html_email_command
    :param subject:
    :param html:
    :param recipients:
    :return: bash_command
    """
    mailgun_api, domain = set_mailgun_api()
    temp_file = 'maligun_html.html'
    local_temp_path = os.getcwd() + '/' + temp_file
    with open(local_temp_path, 'w') as f:
        f.write(html)

    recipient_str = ''
    for recipient in recipients:
        recipient_str += ' -F    to=' + recipient

    bash_command = create_html_email_command(mailgun_api, domain, recipient_str, subject, local_temp_path)
    return bash_command, local_temp_path


def create_html_email_command(mailgun_api, domain, recipient_str, subject, local_temp_path):
    """
    create actual bash command, bash is sensitive to spaces and capitalization so be careful with any changes
    :param mailgun_api:
    :param domain:
    :param recipient_str:
    :param subject:
    :param local_temp_path:
    :return:
    """
    bash_command = """curl -s --user \
                        'api:key-{}' \
                        https://api.mailgun.net/v3/{}/messages \
                        -F    from='Analytic Server <mailgun@{}>' \
                        {} \
                        -F    subject='{}' \
                        -F    html=\"<-\" \"$@\" < {}""".format(mailgun_api, domain, domain,
                                                                recipient_str, subject, local_temp_path)
    return bash_command


def create_html_table(df, html):
    # begin the table
    html += '<table id="tableid">'
    # column headers
    html += "<tr>"
    columns = df.columns
    for col in columns:
        html += "<th>{}</th>".format(col)
    html += "</tr>"
    # table - create row by row
    for i in range(df.shape[0]):
        row = df.iloc[i]
        html += "<tr>"
        for c in range(df.shape[1]):
            html += "<td>{}</td>".format(row[c])
        html += "</tr>"
    # end the table
    html += "</table>"
    # format headers
    return html


def create_table_header(dateobj, report_name, dateformat="%B, %Y"):
    """
    :param dateobj: datetime object
    :param report_name: str
    :return: html
    """
    html = """<!DOCTYPE html>
                <html>
                    <head>
                        <style>
                            #tableid {
                                font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
                                border-collapse: collapse;
                                width: 100%;
                            }
                            #tableid td, #tableid th {
                                border: 1px solid #ddd;
                                padding: 8px;
                            }
                            #tableid tr:nth-child(even){background-color: #f2f2f2;}
                            #tableid tr:hover {background-color: #ddd;}
                            #tableid th {
                                padding-top: 12px;
                                padding-bottom: 12px;
                                text-align: left;
                                background-color: #06AAE2;
                                color: #363636;
                            }
                            h4,#dates{
                                font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
                            }
                        </style>
                    </head>
                <body>"""
    html += "<h4>{}</h4><div></div>".format(report_name)
    html += "<p id=\"dates\">{}</hp><div></div>".format(dateobj.strftime(dateformat))
    return html

# # OTHER # #
