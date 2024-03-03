import os.path
import requests
import sqlite3

from bs4 import BeautifulSoup
from datetime import datetime

def get_bond_data(new_info) -> dict:
    """Parse bond's data from www.minfin.com.ua. 
    Returns dictinary with nominal yeld, coupon payout and maturity date.
    Also create dict {payot_date: amount}."""
    url = f"https://index.minfin.com.ua/finance/bonds/{new_info['ISIN']}/"

    response = requests.get(url=url)
    payout_dates = list()
    if response.status_code == 200:    
        print(f"Bond {new_info['ISIN']} information found. Collecting...")
        page = response.text

        soup = BeautifulSoup(page, "html.parser")
        
        content = soup.dl.find_all("dd")
        bond_data = {
            "ISIN": new_info['ISIN'],
            "Nominal_yield_%": float(content[2].text.replace("\xa0", '').replace(",", '.').replace('%', '')),
            "Maturity_date" : datetime.strptime(content[6].text.replace("\xa0", '').replace(",", '.'), "%d.%m.%Y").date()
        }       
        payout_dates = [datetime.strptime(date.text, '%d.%m.%Y').date() for date in content[7] if '.' in date if datetime.strptime(date, '%d.%m.%Y').date() > new_info['Purchase_date']]
        amount = float(content[3].text.replace("\xa0", '').replace(",", '.'))
        payouts = {payout_date: amount for payout_date in payout_dates}

        print("Collecting succesfull.")
    else:
        print(f"Something going wrong.Status code: {response.status_code}")    
        return {}
    return bond_data, payouts

class DatabaseManager:

    def __init__(self, db_name='bonds_ua.db') -> None:
        self.db_name = db_name
        try:
            self.conn = sqlite3.connect(self.db_name)
            print(f"Connected to existing database '{self.db_name}'.")
        except sqlite3.Error:
            self.create_tables()
            self.conn = sqlite3.connect(self.db_name)
            print("Database created and connected.")
        self.cursor = self.conn.cursor()


    def create_tables(self):
        """Create database and tables."""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("""
            create table if not exists Purchase_info(
                ID integer PRIMARY KEY autoincrement,
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
                Maturity_date date not null,
                FOREIGN KEY (ISIN) references Purchase_info (ISIN)
            );
        """)
        cursor.execute("""
            create table if not exists Payouts(
                ID integer PRIMARY KEY autoincrement,
                ISIN char(12) not null,
                Date date not null,
                Amount numeric(10, 2) not null,
                FOREIGN KEY (ISIN) references Bond_info (ISIN)
            );
        """)
        cursor.execute("""
            create table if not exists Exchange_rate(
                Date date PRIMARY KEY,
                Rate_USD numeric(10, 2),
                Rate_EUR numeric(10, 2)
            );
        """)
        cursor.execute("""
            create table if not exists Analytics(
                ID integer PRIMARY KEY autoincrement,
                Status char(15) default 'Active', 
                Total_purchase numeric(10, 2) not null,
                Holding_period int not null,
                Net_income numeric(10, 2) not null,
                Income_percent numeric(10, 2) not null,
                Year_yield_percent numeric(10, 2) not null,
                FOREIGN KEY (ID) references Purchase_info (ID)
            );
        """)
        cursor.execute("""
            create table if not exists Income_by_year(
                ID integer PRIMARY KEY autoincrement,
                ISIN char(12) not null,
                FOREIGN KEY (ISIN) references Purchase_info (ISIN) 
            );
        """)
        conn.commit()
        cursor.close()
        conn.close()
        
    def add_purchase_row(self, new):
        """Insert new purchase and start filling cascade."""
        # Insert new purchase entering data 
        query = "insert into Purchase_info (ISIN, Purchase_date, Quantity, Price, Reinvest, Broker, Fee) values (?, ?, ?, ?, ?, ?, ?);"
        isin = new['ISIN']
        self.cursor.execute(query, list(new.values()))
        self.conn.commit()
        print(f"New purchase from {new['Purchase_date']} added to DataBase!")
        # Step 1: filling Bonds_info and Payouts if ISIN is new. 
        if not self.bond_data_exists(isin):
            bonds_info, payments = get_bond_data(new)
            self.add_bond_info(bonds_info)
            self.add_payouts(code=isin, payouts_data=payments)
        else:
            print(f"Bond {isin} information is already in DataBase.")
        # Step 2: filling Analitycs
        self.add_analytics(new)
        # Step 3: filling Income_by_year
        # Step 4: filling Exchange_rate
        

    def bond_data_exists(self, code) -> bool:
        # Return True if bonds data exists in db
        query = "select * from Bonds_info where ISIN = ?;"
        self.cursor.execute(query, (code,))
        result = self.cursor.fetchone()
        return True if result else False
    
    def add_bond_info(self, data):
        query = "insert into Bonds_info (ISIN, Nominal_yield, Maturity_date) values (?, ?, ?);"
        self.cursor.execute(query, list(data.values()))
        self.conn.commit()
        print(f"Bond {data['ISIN']} information added.")

    def add_payouts(self, code, payouts_data):
        for date, amount in payouts_data.items():
            query = "insert into Payouts (ISIN, Date, Amount) values (?, ?, ?);"
            self.cursor.execute(query, (code, date, amount))
        self.conn.commit()
        print(f"Added {code} payouts.")

    def add_analytics(self, purchase):
        # Count total amount of purchase
        total = round(purchase['Price'] * purchase['Quantity'], 2)
        # Get maturity date from db for calculation holding period
        query = "select Maturity_date from Bonds_info where ISIN = ?;"
        self.cursor.execute(query, (purchase['ISIN'],))
        end = self.cursor.fetchone()[0]
        start = purchase['Purchase_date']
        period = (datetime.strptime(end, "%Y-%m-%d").date() - start).days
        # Get summ of all payouts fot current purchase
        query = "select sum(Amount) from Payouts where ISIN = ? and Date > ?;"
        self.cursor.execute(query, (purchase['ISIN'], purchase['Purchase_date']))
        payouts = self.cursor.fetchone()[0]
        payouts = float(payouts) if payouts is not None else 0
        # Calculating net income
        net_income = round(purchase['Quantity'] * (1000 + payouts) - total, 2)
        # Now can calculate income % and year yield in %
        income_percent = round(net_income/total * 100, 2)
        year_yield = round(income_percent*365/period, 2)
        # Finally data ready for record in db
        query = "insert into Analytics (Total_purchase, Holding_period, Net_income, Income_percent, Year_yield_percent) values (?, ?, ?, ?, ?);"
        self.cursor.execute(query, (total, period, net_income, income_percent, year_yield))
        self.check_status()
        self.conn.commit()
        print(f"Add row for purchase from {purchase['Purchase_date']}.")

    def check_status(self):
        query = """
        update Analytics 
        set Analytics.Status = 'Paid of' 
        where exists (
            select 1
            from Bonds_info
            where Analytics.ISIN = Bonds_info.ISIN
            and date(now) >= Bonds_info.Maturity_date;"""
        self.cursor.execute(query)


    def close_connection(self):
        self.cursor.close()
        self.conn.close()
    