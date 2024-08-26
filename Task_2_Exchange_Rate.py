import requests
import datetime
import sqlite3
import pandas as pd

class ExchangeRate:
    def __init__(self, api_key, base_currency='EUR', db_name='exchange_rates.db'):
        self.api_key = api_key
        self.base_url = "http://data.fixer.io/api/"
        self.base_currency = base_currency  # Set the default base currency
        self.conn = sqlite3.connect(db_name)
        self.create_table()

    def create_table(self):
        with self.conn:
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS rates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    base_code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    rate REAL NOT NULL,
                    currency_code TEXT NOT NULL
                )
            ''')

    def get_current_rates(self):
        endpoint = f"{self.base_url}latest"
        params = {
            'access_key': self.api_key,
            'base': self.base_currency  # Use the instance's base currency
        }
        response = requests.get(endpoint, params=params)
        data = response.json()
        if data['success']:
            return data['rates']
        else:
            raise Exception(f"Error: {data['error']['info']}")

    def get_historical_rates(self, start_date=None, end_date=None, base_currency='EUR'):
        today = datetime.date.today()
        two_years_ago = today - datetime.timedelta(days=730)  # Approximate two years

        # Ensure dates are datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

        # Set default date range to the last two years
        if start_date is None:
            start_date = two_years_ago
        if end_date is None:
            end_date = today

        # Validate the date range
        if start_date < two_years_ago or end_date > today:
            raise ValueError(f"Dates must be within the last two years. "
                             f"Provided range: {start_date} to {end_date}. "
                             f"Valid range: {two_years_ago} to {today}.")

        if start_date > end_date:
            raise ValueError("The start_date must be before or equal to the end_date.")

        rates = {}
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            endpoint = f"{self.base_url}{date_str}"
            params = {
                'access_key': self.api_key,
                'base': base_currency
            }
            response = requests.get(endpoint, params=params)
            data = response.json()

            if data['success']:
                rates[date_str] = data['rates']
            else:
                print(f"Failed to retrieve data for {date_str}: {data['error']['info']}")

            # Move to the next date
            current_date += datetime.timedelta(days=1)

        return rates

    def store_rates(self, date, rates):
        with self.conn:
            for currency_code, rate in rates.items():
                self.conn.execute('''
                    INSERT INTO rates (base_code, date, rate, currency_code)
                    VALUES (?, ?, ?, ?)
                ''', (self.base_currency, date, rate, currency_code))  # Use the instance's base currency

    def store_historical_rates_range(self, start_date, end_date):
        # Ensure start_date and end_date are datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

        today = datetime.date.today()
        two_years_ago = today - datetime.timedelta(days=730)  # Approximate two years (365 * 2)

        # Validate the date range
        if start_date < two_years_ago or end_date > today:
            raise ValueError(f"Dates must be within the last two years. "
                             f"Provided range: {start_date} to {end_date}. "
                             f"Valid range: {two_years_ago} to {today}.")

        if start_date > end_date:
            raise ValueError("The start_date must be before or equal to the end_date.")

        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            try:
                historical_rates = self.get_historical_rates(date_str, date_str, base_currency=self.base_currency)

                # Storing data in DB
                self.store_rates(date_str, historical_rates[date_str])

                print(f"Exchange rates for {self.base_currency} on {date_str} have been stored in the database.")
            except Exception as e:
                print(f"Failed to retrieve or store data for {date_str}: {e}")

            # Move to the next date
            current_date += datetime.timedelta(days=1)

    def display_rates(self, rates):
        for currency, rate in sorted(rates.items()):
            print(f"{currency}: {rate:.4f}")

    def display_historical_rates(self, date):
        try:
            rates = self.get_historical_rates(date)
            print(f"Exchange Rates on {date} (Base: {self.base_currency})")
            print("-" * 40)
            self.display_rates(rates)
        except Exception as e:
            print(str(e))

    def display_current_rates(self):
        try:
            rates = self.get_current_rates()
            print(f"Current Exchange Rates (Base: {self.base_currency})")
            print("-" * 40)
            self.display_rates(rates)
        except Exception as e:
            print(str(e))

    def display_rates_for_date_range(self, start_date, end_date):
        try:
            current_date = start_date
            while current_date <= end_date:
                self.display_historical_rates(current_date.strftime('%Y-%m-%d'))
                current_date += datetime.timedelta(days=1)
        except Exception as e:
            print(str(e))

    def calculate_average_rate(self, currency_code, start_date, end_date):
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        query = '''
            SELECT date, rate FROM rates
            WHERE currency_code = ?
            AND date BETWEEN ? AND ?
            AND base_code = ?
        '''
        df = pd.read_sql_query(query, self.conn, params=(currency_code, start_date_str, end_date_str, self.base_currency))

        if df.empty:
            print(f"No exchange rate data found for {currency_code} between {start_date_str} and {end_date_str}.")
            return None

        # Calculate the average rate using Pandas
        average_rate = df['rate'].mean()
        return average_rate

    def display_average_rate(self, currency_code, start_date, end_date):
        average_rate = self.calculate_average_rate(currency_code, start_date, end_date)
        if average_rate is not None:
            print(f"Average exchange rate for {currency_code} from {start_date} to {end_date} (Base: {self.base_currency}): {average_rate:.4f}")

    def close(self):
        self.conn.close()