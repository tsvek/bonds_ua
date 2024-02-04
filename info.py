import csv
import os.path
import requests

from bs4 import BeautifulSoup

code = "UA4000227656" # input("Entern bond's ID: ")

def get_bond_data(bond_id) -> dict:
    """Parse bond's data from www.minfin.com.ua. Returns dictinary."""
    url = f"https://index.minfin.com.ua/finance/bonds/{code}/"

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
    if os.path.isfile('bonds_info.csv'):
        with open('bonds_info.csv', mode='r', encoding='utf-8', newline='') as file:
            reader = csv.DictReader(file, delimiter=';')
            isins = [row['ISIN'] for row in reader]
        return bond_isin in isins
    return False

def save_bond_info(info):
    mode = 'a+' if os.path.isfile('bonds_info.csv') else 'w'
    with open("bonds_info.csv", mode=mode, encoding="utf-8", newline='') as file:
        writer = csv.DictWriter(file, fieldnames=info.keys(), delimiter=';')
        if mode == 'w':
            writer.writeheader()
            print("Created new file with bonds information and added columns name.")
        writer.writerow(info)
        print(f"Bond {info['ISIN']} information added.")

if bond_info_exists(code):
    print(f"Bond {code} informations already exists.")
else:
    bond_info = get_bond_data(code)
    save_bond_info(bond_info)