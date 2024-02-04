import csv
import os.path
import requests

from bs4 import BeautifulSoup


def get_bond_data(bond_id) -> dict:
    """Parse bond's data from www.minfin.com.ua. Returns dictinary."""
    url = f"https://index.minfin.com.ua/finance/bonds/{bond_id}/"

    response = requests.get(url=url)
    if response.status_code == 200:    
        print(f"Bond {bond_id} information found. Collecting...")
        page = response.text

        soup = BeautifulSoup(page, "html.parser")
        
        headlines = soup.dl.find_all("dt")
        content = soup.dl.find_all("dd")
        
        bond_data = {"ISIN": bond_id}
        for id in range(len(headlines)):
            if id in [5, 7]:
                bond_data[headlines[id].text] = [date.text for date in content[id] if '.' in date]
            else:
                bond_data[headlines[id].text] = content[id].text.replace("\xa0", '').replace(",", '.').split()[0]
        print("Collecting succesfull.")
    else:
        print(f"Something going wrong.Status code: {response.status_code}")    
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
        'Date': input("Enter date of purchase(dd.mm.YYYY): "),
        'Price': float(input("Enter the bond's price: ")),
        'Reinvest': float(input("Enter the amount of re-investment: ")),
        'Number': int(input("Enter the number of bond purchased: ")),
    }

    save_bond_info(purchase_info, 'purchase')

    if bond_info_exists(purchase_info['ISIN']):
        print(f"Bond {purchase_info['ISIN']} informations already exists.")
    else:
        bond_info = get_bond_data(purchase_info['ISIN'])
        save_bond_info(bond_info, 'collect')