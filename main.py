import csv
import os.path
import requests

from bs4 import BeautifulSoup
from datetime import datetime

def get_bond_data(bond_id) -> dict:
    """Parse bond's data from www.minfin.com.ua. 
    Returns dictinary with nominal yeld, coupon payout and maturity date.
    Also put actuasls payout dayes in global payout_dates list."""
    global payout_dates
    url = f"https://index.minfin.com.ua/finance/bonds/{bond_id}/"

    response = requests.get(url=url)
    if response.status_code == 200:    
        print(f"Bond {bond_id} information found. Collecting...")
        page = response.text

        soup = BeautifulSoup(page, "html.parser")
        
        content = soup.dl.find_all("dd")
        payout_dates = [datetime.strptime(date.text, '%d.%m.%Y') for date in content[7] if '.' in date if datetime.strptime(date, '%d.%m.%Y').date() > purchase_info['Purchase_date']]
        bond_data = {"Nominal_yield_%": float(content[2].text.replace("\xa0", '').replace(",", '.').replace('%', '')),
                     "Payout_amount": float(content[3].text.replace("\xa0", '').replace(",", '.')),
                     "Maturity_date" : datetime.strptime(content[6].text.replace("\xa0", '').replace(",", '.'), "%d.%m.%Y").date()
        }       

        print("Collecting succesfull.")
    else:
        print(f"Something going wrong.Status code: {response.status_code}")    
        return {}
    return bond_data

def bond_info_exists(bond_isin) -> bool:
    """Check if bond information exists. Return True or False."""
    if os.path.isfile('bonds_info.csv'):
        with open('bonds_info.csv', mode='r', encoding='utf-8', newline='') as file:
            reader = csv.DictReader(file, delimiter=';')
            isins = [row['ISIN'] for row in reader]
        return bond_isin in isins
    return False

def save_bond_info(info, action) -> None:
    """Save bond information in csv file. Create new and write headers if file does'n exist. Dictinary and action(collect by default / purchase) requires."""
    mode = 'a+' if os.path.isfile('bonds_info.csv') else 'w'
    file_name = "bonds_info.csv" if action == 'collect' else 'purchased_info.csv'
    done_print = f"Bond {info['ISIN']} information added." if action == 'collect' else f"Bond {info['ISIN']} purchase information for {info['Date']} has been saved."
    with open(file_name, mode=mode, encoding="utf-8", newline='') as file:
        writer = csv.DictWriter(file, fieldnames=info.keys(), delimiter=';')
        if mode == 'w':
            writer.writeheader()
            print("Created new file with bonds information and added columns name.")
        writer.writerow(info)
        print(done_print)

game_on = True
while game_on:
    new = input("Do you want to add new purchase? Y or N: ")
    if new.lower() == "n":
        game_on = False
        break
    purchase_info = {
        'ISIN': input("Enter bond's code(UAxxxxxxxxxx): "),
        'Broker': input("Enter the broker name: "),
        'Purchase_date': datetime.strptime(input("Enter date of purchase(dd.mm.YYYY): "), '%d.%m.%Y').date(), 
        'Price': float(input("Enter the bond's price: ")),
        'Number': int(input("Enter the number of bond purchased: ")),
    }
    purchase_info['Purchase_amount'] = round(purchase_info['Price']*purchase_info['Number'], 2)
    purchase_info.update({
        'Reinvest': float(input("Enter the amount of re-investment: ")),
        'Fee': float(input("Enter the fee: "))
    })
    
    
    url = f"https://index.minfin.com.ua/finance/bonds/{purchase_info['ISIN']}/"

    response = requests.get(url=url)
    payout_dates = []
    if response.status_code == 200:    
        print(f"Bond {purchase_info['ISIN']} information found. Collecting...")
        page = response.text

        soup = BeautifulSoup(page, "html.parser")
        
        content = soup.dl.find_all("dd")
        payout_dates = [datetime.strptime(date.text, '%d.%m.%Y') for date in content[7] if '.' in date if datetime.strptime(date, '%d.%m.%Y').date() > purchase_info['Purchase_date']]
        bond_data = {"Nominal_yield_%": float(content[2].text.replace("\xa0", '').replace(",", '.').replace('%', '')),
                     "Payout_amount": float(content[3].text.replace("\xa0", '').replace(",", '.')),
                     "Maturity_date" : datetime.strptime(content[6].text.replace("\xa0", '').replace(",", '.'), "%d.%m.%Y").date()
        }       

        print("Collecting succesfull.")
    else:
        print(f"Something going wrong.Status code: {response.status_code}")    
        continue

    purchase_info.update(bond_data)
    profit_info = {
        'Holding_period_days': (purchase_info['Maturity_date'] - purchase_info['Purchase_date']).days,
        'Net_income': round(purchase_info['Number'] * (1000 + purchase_info['Payout_amount'] * len(payout_dates)) - purchase_info['Purchase_amount'], 2),
    }
    purchase_info.update(profit_info)
    purchase_info['Income_%'] = round(purchase_info['Net_income']/purchase_info['Purchase_amount']*100, 2)
    purchase_info['Year_yield_%'] = round(purchase_info['Income_%']*365/purchase_info['Holding_period_days'], 2)
    #purchase_info['Status'] = 'Active' if date.today() < purchase_info['Maturity_date'] else purchase_info['Number'] * 1000
    for year in range(min(payout_dates).year, max(payout_dates).year + 1):
        purchase_info[f'Payout_yield_{year}'] = purchase_info['Number'] * purchase_info['Payout_amount'] * sum(1 for payout in payout_dates if payout.year == year)
    
    print(purchase_info)
