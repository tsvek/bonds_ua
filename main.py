import csv
import requests

from bs4 import BeautifulSoup

code = input("Entern bond's ID: ")

def get_bond_data(bond_id) -> dict:
    """Parse bond's data from www.minfin.com.ua. Returns dictinary."""
    url = f"https://index.minfin.com.ua/finance/bonds/{code}/"

    response = requests.get(url=url)
    page = response.text

    soup = BeautifulSoup(page, "html.parser")
    
    headlines = soup.dl.find_all("dt")
    content = soup.dl.find_all("dd")
    
    bond_data = {"ISIN": bond_id}
    for id in range(len(headlines)):
        if id in [5, 7]:
            bond_data[headlines[id].text] = [date.text for date in content[id] if '.' in date]
        else:
            bond_data[headlines[id].text] = content[id].text.replace("\xa0", '').split()[0]

    return bond_data

bond_info = get_bond_data(code)
print(bond_info)



