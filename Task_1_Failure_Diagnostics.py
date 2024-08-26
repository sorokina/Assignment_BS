import datetime
import json
import urllib.request
import urllib.parse
import http
import http.cookiejar
import psycopg2
import logging
import sys
import traceback


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s',
                    handlers=[
                        logging.StreamHandler(sys.stdout),
                        logging.FileHandler('script_diagnostic.log')
                    ])


def backend_authenticate():
    global url_opener, auth_token
    try:
        logging.info("Starting authentication process")
        cookie_jar = http.cookiejar.CookieJar()
        url_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
        login_params = {'username': 'admin', 'password': 'sdaf@#@#hkjahsd2'}
        logging.debug(f"Login parameters: {login_params}")
        post_data = json.dumps(login_params).encode('utf-8')
        request = urllib.request.Request('https://api.retailer.com/login')
        request.add_header('Content-Type', 'application/json')
        request.add_header('Accept', 'application/json')

        with url_opener.open(request, post_data) as response:
            auth_json = response.read(1024).decode('utf-8')
            if response.read(1024) != b'':
                raise Exception('Downloaded JSON is larger than 1 MiB')
            auth_data = json.loads(auth_json)
            auth_token = auth_data['token']
            logging.info("Authentication successful")
    except Exception as e:
        logging.error("Authentication failed: %s", e)
        logging.debug(traceback.format_exc())
        raise


def download_data(start_date, end_date):
    try:
        logging.info(f"Downloading data from {start_date} to {end_date}")
        url = ('https://api.retailer.com/reporting/orders/?' + urllib.parse.urlencode(
            {'start': start_date, 'end': end_date}))
        request = urllib.request.Request(url)
        request.add_header('Authorization', auth_token)

        with url_opener.open(request) as response:
            orders_json = response.read(104857600).decode('utf-8')
            if response.read(1024) != b'':
                raise Exception('Downloaded JSON is larger than 100 MiB')
            rows = json.loads(orders_json)
            if rows[0] != ['orderid', 'customerid', 'orderdate', 'orderstatus', 'totalamount',
                           'paymentmethod', 'shippingaddress', 'billingaddress', 'shippingfee',
                           'taxamount', 'itemlist']:
                logging.warning("Unexpected data structure received")
                return []
            logging.info("Data download successful")
            return rows[1:]
    except urllib.error.URLError as e:
        logging.error("Network error during data download: %s", e)
        logging.debug(traceback.format_exc())
        raise
    except Exception as e:
        logging.error("Data download failed: %s", e)
        logging.debug(traceback.format_exc())
        raise


def insert_in_db_dwh(conn, rows):
    try:
        logging.info("Inserting data into the database")
        cur = conn.cursor()
        for row in rows:
            logging.debug(f"Inserting row: {row}")
            cur.execute("""
                INSERT INTO warehouse.orders
                (orderid, customerid, orderdate, orderstatus, totalamount,
                paymentmethod, shippingaddress, billingaddress, shippingfee,
                taxamount, itemlist)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, row)
        conn.commit()
        cur.close()
        logging.info("Data insertion into DB successful")
    except psycopg2.Error as e:
        logging.error("Database error during data insertion: %s", e)
        logging.debug(traceback.format_exc())
        raise
    except Exception as e:
        logging.error("Data insertion into DB failed: %s", e)
        logging.debug(traceback.format_exc())
        raise


def main():
    try:
        logging.info("Script execution started")
        end_date = datetime.date.today() - datetime.timedelta(days=1)
        start_date = end_date - datetime.timedelta(days=1)
        backend_authenticate()
        db_conn = psycopg2.connect(
            host='dwh.retailer.com',
            dbname='dwh',
            user='dbadmin',
            password='fasyyyyy3tg*&'
        )
        logging.info("Database connection established")
        rows = download_data(start_date, end_date)
        if rows:
            insert_in_db_dwh(db_conn, rows)
        else:
            logging.warning("No data to insert into DB")
    except Exception as e:
        logging.critical("Script execution failed: %s", e)
        logging.debug(traceback.format_exc())
        sys.exit(1)
    finally:
        db_conn.close()
        logging.info("Database connection closed")


if __name__ == '__main__':
    main()