import requests
import datetime

from dotenv import load_dotenv
import os

from Task_2_Exchange_Rate import ExchangeRate  # Adjust the import based on your file structure

load_dotenv()

api_key= os.getenv('FIXER_API_KEY')

exchange_rate = ExchangeRate(api_key)

# Method "get_current_rates"
rates = exchange_rate.get_current_rates()
print("Rates:")
print(rates)
start_date = datetime.date(2024, 1, 1)
end_date = datetime.date(2024, 1, 5)
# Display exchange rates for the defined date range (e.g., USD as base currency)
exchange_rate.display_rates_for_date_range(start_date,end_date)

# Store historical rates for the date range

start_date = datetime.date(2024, 1, 1)
end_date = datetime.date(2024, 1, 5)

# Store historical rates for the date range
exchange_rate.store_historical_rates_range(start_date, end_date)

# Calculate and display the average exchange rate for a specific currency over the date range
exchange_rate.display_average_rate('USD', start_date, end_date)
# Close the database connection
exchange_rate.close()