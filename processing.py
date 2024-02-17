import pandas as pd

from ast import literal_eval
from datetime import date

#pd.set_option('display.max_column', None)
purchase = pd.read_csv('purchased_info.csv', delimiter=';')
info = pd.read_csv('bonds_info.csv', delimiter=';')

# Merge our two files into one table.
result = pd.merge(purchase, info, on='ISIN', how='inner', )
# Chahge the headers
new_headers = ['ISIN', 'Broker', 'Purchase_date', 'Price',  'Number', 'Reinvest', 'Fee', 'Nominal_yield_%',  'Coupon_amount', 'Maturity_date', 'Coupon_payment_date']
result.columns = new_headers

# Change types of columns
result['Purchase_date'] = pd.to_datetime(result['Purchase_date'], format="%d.%m.%Y").dt.date
result['Maturity_date'] = pd.to_datetime(result['Maturity_date'], format="%d.%m.%Y").dt.date
result[['ISIN', 'Broker']].astype(str)
result['Nominal_yield_%'] = pd.to_numeric(result['Nominal_yield_%'].apply(lambda x: x.replace('%', '')))

# Add purchare amont column
result.insert(loc=5, column='Purchase_amount', value=result['Price']*result['Number'])

# Add colunn with holding period
result.insert(loc=11, column='Holding_period_days', value=(result['Maturity_date'] - result['Purchase_date']))
# Add status column
result.insert(loc=12, column='Status', value=result.apply(lambda row: 'Active' if date.today() < row['Maturity_date'] else row['Number'] * 1000, axis=1))

result['Coupon_payment_date'] = result['Coupon_payment_date'].apply(lambda x: pd.to_datetime(literal_eval(x), format='%d.%m.%Y'))
# Value from this df will be need for future
dates = pd.DataFrame()
dates[['Maturity_date', 'Purchase_date']] = result[['Maturity_date', 'Purchase_date']]
# Remove unused date from coupon_payment and split coupon payment by years
for year in range(min(result['Purchase_date']).year, max(result['Maturity_date']).year+1):
    dates[f'Coupon_payout_{year}'] = result.apply(lambda row: [date.date() for date in row['Coupon_payment_date'] if date.year == year and date.date() > row['Purchase_date']], axis=1)
    
    # Add column with payout amount by year
    result[f'Payout_amount_{year}'] = result['Coupon_amount'] * dates[f'Coupon_payout_{year}'].apply(lambda x: len(x))
# Try to get all unique dates from our data
explode_dates = dates.apply(lambda col: col.explode().reset_index(drop=True))
str_dates = explode_dates.applymap(str)
dates_list = set(str_dates.values.flatten())
result = result.drop('Coupon_payment_date', axis=1)

# TODO: Add net_income column
# TODO: Add real_yield column
print(result)
#print(dates_list)

