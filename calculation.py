import pandas as pd

data = pd.read_csv('ready.csv', delimiter=';')

# TODO Total amount
total = round(data['Purchase_amount'].sum() - data.loc[data['Status'] != "Active", 'Status', ].astype(int).sum(), 2)
print(f"Total invest amount = {total}")
# TODO Average bond price
average = round(data['Price'].mean(), 2)
print(f"The average bond price = {average}")
# TODO Bonds number
number = data.loc[data['Status'] == "Active", 'Number'].sum()
print(f'The total bonds number = {number}') 
# TODO Reinvest amount
reinvest = round(data['Reinvest'].sum(), 2)
print(f"The reinvest amount = {reinvest}")
# TODO Invest period
period = (pd.to_datetime(data['Maturity_date'].max(), format='%Y-%m-%d').date() - pd.to_datetime(data['Purchase_date'].min(), format='%Y-%m-%d').date()).days
print(f'The invest period = {period} days') 
# TODO Years payout
total_payout = 0 
for col in data.columns:
    if 'Payout' in col:
        print(f"""{col.split('_')[1]} year:  - yield = {round(data[col].sum()/total*100, 2)}%
            - payout = {round(data[col].sum(), 2)}""")
        total_payout += round(data[col].sum(), 2)

print(f"Total payout = {total_payout}")

