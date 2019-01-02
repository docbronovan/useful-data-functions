################
# # AVERAGES # #
################
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

""" to check for outliers in numeric column of a dataframe. You may set your own threshold
"""
points = df['column_name']
data = is_outlier(points, thresh=3.5)


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
    print 'setup google client'
    scope = ['https://spreadsheets.google.com/feeds']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)
    return client

def get_spreadsheet_values(client, gsheet_name):
    """
    get values from google spreadsheet
    :param sheet: google sheet object
    :return: dataframe
    """
    print 'getting spreadsheet values'
    sheet = client.open(gsheet_name).sheet1
    # Find a workbook by name and open the first sheet. 
    # Gspread only allows you to open a sheet named sheet1
    try:
        list_of_hashes = sheet.get_all_records()
    except AttributeError:
        send_error_email('get_spreadsheet_values: AttributeError', [])
        list_of_hashes = []
    sheet_values_df = pd.DataFrame(list_of_hashes)
    return sheet_values_df



######################
# # # POSTGRESQL # # #
######################
import psycopg2
import pandas as pd
def set_cursor():
    """ relies on set_postgres_params which reads sensative parameters from secrets.py """
    postgres_host, postgres_user, postgres_pw, postgres_db = set_postgres_params()
    conn = psycopg2.connect(host=postgres_host, database=postgres_db, user=postgres_user, password=postgres_pw)
    cur = conn.cursor()
    return conn, cur


def check_and_insert_postgres(cur, index_col, table_name, df_to_insert):
    """
    Gets index value from table to check against. If not matched then we need to make a new row in the table.
    :param cur: cursor object
    :param index_col: str - col to check whether to insert
    :param table_name: str - table you want to check
    :param df_to_insert: dataframe with values you want to check if already inserted
    :return:
    """
    sql = 'select {} from {};'.format(index_col, table_name)
    column_names_str = ','.join(list(df_to_insert.columns.values))
    alread_written = return_sql(sql, cur, column_names_str)
    for value in df_to_insert[index_col]:
        if value not in already_written:
            row_values = df_to_insert.loc[df_to_insert[index_col] == value].iloc[0].values
            row_values_str = '\',\''.join([unicode(i) for i in row_values])
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
