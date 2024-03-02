import os.path
import requests
import sqlite3

from bs4 import BeautifulSoup
from datetime import datetime

def get_bond_data(bond_isin) -> dict:
    """Parse bond's data from www.minfin.com.ua. 
    Returns dictinary with nominal yeld, coupon payout and maturity date.
    Also put actuasls payout dayes in global payout_dates list."""
    url = f"https://index.minfin.com.ua/finance/bonds/{bond_isin}/"

    response = requests.get(url=url)
    payout_dates = list()
    if response.status_code == 200:    
        print(f"Bond {bond_isin} information found. Collecting...")
        page = response.text

        soup = BeautifulSoup(page, "html.parser")
        
        content = soup.dl.find_all("dd")
        bond_data = {
            "ISIN": bond_isin,
            "Nominal_yield_%": float(content[2].text.replace("\xa0", '').replace(",", '.').replace('%', '')),
            "Maturity_date" : datetime.strptime(content[6].text.replace("\xa0", '').replace(",", '.'), "%d.%m.%Y").date()
        }       
        payout_dates = [datetime.strptime(date.text, '%d.%m.%Y') for date in content[7] if '.' in date if datetime.strptime(date, '%d.%m.%Y').date() > new_purchase['Purchase_date']]
        amount = float(content[3].text.replace("\xa0", '').replace(",", '.'))
        payouts = {payout_date: amount for payout_date in payout_dates}

        print("Collecting succesfull.")
    else:
        print(f"Something going wrong.Status code: {response.status_code}")    
        return {}
    return bond_data, payouts

class DatabaseManager:

    def __init__(self, db_name='bonds_ua.db') -> None:
        if not os.path.isfile(db_name):
            self.create_tables(db_name)
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()


    def create_tables(self, db_name):
        """Create database and tables."""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("""
            create table if not exists Purchase_info(
                ID int PRIMARY KEY,
                ISIN char(12) not null,
                Purchase_date date not null,
                Quantity int not null,
                Price numeric(10, 2) not null,
                Reinvest numeric(10, 2) not null,
                Broker char(20) not null,
                Fee numeric(10, 2) default 0.0
            );
        """)
        cursor.execute("""
            create table if not exists Bonds_info(
                ISIN char(12) PRIMARY KEY, 
                Nominal_yield numeric(10, 2) not null,
                Maturity_day date not null,
                FOREIGN KEY (ISIN) references Purchase_info (ISIN)
            );
        """)
        cursor.execute("""
            create table if not exists Payouts(
                ID int PRIMARY KEY,
                ISIN char(12) not null,
                Date date not null,
                Amount numeric(10, 2) not null,
                FOREIGN KEY (ISIN) references Bond_info (ISIN)
            );
        """)
        cursor.execute("""
            create table if not exists Exchange_rate(
                ID int PRIMARY KEY,
                Date date not null,
                Rate_USD numeric(10, 2),
                Rate_EUR numeric(10, 2)
            );
        """)
        cursor.execute("""
            create table if not exists Analytics(
                ID int PRIMARY KEY,
                Status char(15) not null, 
                Total_purchase numeric(10, 2) not null,
                Holding_periond int not null,
                Net_income numeric(10, 2) not null,
                Income_percent numeric(10, 2) not null,
                Year_yield_percent numeric(10, 2) not null,
                FOREIGN KEY (ID) references Purchase_info (ID)
            );
        """)
        cursor.execute("""
            create table if not exists Income_by_year(
                ID int PRIMARY KEY,
                ISIN char(12) not null,
                FOREIGN KEY (ISIN) references Purchase_info (ISIN) 
            );
        """)
        conn.commit()
        
    def add_purchase_row(self, info_list):
        query = "insert into Purchase_info (ISIN, Purchase_date, Quantity, Price, Reinvest, Broker, Fee) values (?, ?, ?, ?, ?, ?, ?);"
        isin = info_list[0]
        self.cursor.execute(query, info_list)
        self.conn.commit()
        print(f"New purchase from {info_list[1]} added to DataBase!")
        if not self.bond_data_exists(isin):
            bonds_info, payments = get_bond_data(isin)
            self.add_bond_info(bonds_info)
        else:
            print(f"Info about bond {isin} is already in DataBase.")

    def add_bond_info(self, data):
        query = "insert into Bonds_info (ISIN, Nominal_yield, Maturity_date) value (?, ?, ?);"
        self.cursor(query, data)
        self.conn.commit()

    def bond_data_exists(self, code):
        query = "select * from Bonds_info where ISIN = ?;"
        result = self.cursor.execute(query, code)
        return True if len(result) > 0 else False

    def close_connection(self):
        self.cursor.close()
        self.conn.close()
    