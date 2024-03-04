import os.path
import requests
import sqlite3

from bs4 import BeautifulSoup
from datetime import datetime
from key import key
from time import sleep

RATE_API_KEY = key

def get_bond_data(new_info) -> dict:
    """Parse bond's data from www.minfin.com.ua. 
    Returns dictinary with nominal yeld, coupon payout and maturity date.
    Also create dict {payot_date: amount}."""
    url = f"https://index.minfin.com.ua/finance/bonds/{new_info['ISIN']}/"

    response = requests.get(url=url)
    payout_dates = list()
    if response.status_code == 200:    
        print(f"{datetime.now().time()}     Bond {new_info['ISIN']} information found. Collecting...")
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

        print(f"{datetime.now().time()}     Collecting succesfull.")
    else:
        print(f"{datetime.now().time()}     Something going wrong.Status code: {response.status_code}")    
        return {}
    return bond_data, payouts

def get_exchange_rate(date) -> list:
    """Get exchange rate for a given date and currency. Return number in float type."""
    url = f"https://api.currencyapi.com/v3/historical"
    params = {
        'date': date,
        'apikey': RATE_API_KEY,
        'base_currency': "UAH",
        'currencies': "EUR,USD"
    }
    response = requests.get(url=url, params=params)
    data = response.json()['data']
    usd = data['USD']['value'] 
    eur = data['EUR']['value']
    print(f"{datetime.now().time()}      Get them!!=)")
    return [usd, eur]

class DatabaseManager:

    def __init__(self, db_name='bonds_ua.db') -> None:
        self.db_name = db_name
        self.current_data = None
        self.count = 0
        if not os.path.isfile(db_name):
            self.create_tables()
            print(f"{datetime.now().time()}    Database created and connected.")
        else:
            print(f"{datetime.now().time()}    Connected to existing database '{self.db_name}'.")
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()

    def create_tables(self) -> None:
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
                Rate_USD real default 0.0,
                Rate_EUR real default 0.0
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
        # Concluded that don't need this table. It's easier to calculate data saparetly using python.
        # cursor.execute("""
        #     create table if not exists Income_by_year(
        #         ID integer PRIMARY KEY autoincrement,
        #         ISIN char(12) not null,
        #         FOREIGN KEY (ISIN) references Purchase_info (ISIN) 
        #     );
        # """)
        conn.commit()
        cursor.close()
        conn.close()
        
    def add_purchase_row(self, new) -> None:
        """Insert new purchase and start filling cascade."""
        self.current_data = new
        # Insert new purchase entering data 
        query = "insert into Purchase_info (ISIN, Purchase_date, Quantity, Price, Reinvest, Broker, Fee) values (?, ?, ?, ?, ?, ?, ?);"
        self.cursor.execute(query, tuple(self.current_data.values()))
        self.conn.commit()
        print(f"{datetime.now().time()}    New purchase from {self.current_data['Purchase_date']} added to DataBase!")
        self.add_exchange_date(self.current_data['Purchase_date'])
        # Step 1: filling Bonds_info and Payouts if ISIN is new. 
        if not self.bond_data_exists():
            bonds_info, payments = get_bond_data(self.current_data)
            self.add_bond_info(bonds_info)
            self.add_exchange_date(bonds_info['Maturity_date'])
            self.add_payouts(payouts_data=payments)
            for p_date in payments.keys():
                self.add_exchange_date(p_date)
        else:
            print(f"{datetime.now().time()}        Bond {self.current_data['ISIN']} information is already in DataBase.")
        # Step 2: filling Analitycs
        self.add_analytics()

        # Step 3: filling Exchange_rate
        self.add_exchange_rate()
        
    def bond_data_exists(self) -> bool:
        """Return True if bonds data exists in db"""
        query = "select * from Bonds_info where ISIN = ?;"
        self.cursor.execute(query, (self.current_data['ISIN'],))
        result = self.cursor.fetchone()
        return True if result else False
    
    def add_bond_info(self, data) -> None:
        """Add row with bonds info."""
        query = "insert into Bonds_info (ISIN, Nominal_yield, Maturity_date) values (?, ?, ?);"
        self.cursor.execute(query, tuple(data.values()))
        self.conn.commit()
        print(f"{datetime.now().time()}    Bond {data['ISIN']} information added.")

    def add_payouts(self, payouts_data) -> None:
        """Add bond payout dates and amoun."""
        for date, amount in payouts_data.items():
            query = "insert into Payouts (ISIN, Date, Amount) values (?, ?, ?);"
            self.cursor.execute(query, (self.current_data['ISIN'], date, amount))
        self.conn.commit()
        print(f"{datetime.now().time()}    Added {self.current_data['ISIN']} payouts.")

    def add_analytics(self) -> None:
        """Calculate and fill analitycs part."""
        # Count total amount of purchase
        total = round(self.current_data['Price'] * self.current_data['Quantity'], 2)
        # Get maturity date from db for calculation holding period
        query = "select Maturity_date from Bonds_info where ISIN = ?;"
        self.cursor.execute(query, (self.current_data['ISIN'],))
        end = self.cursor.fetchone()[0]
        start = self.current_data['Purchase_date']
        period = (datetime.strptime(end, "%Y-%m-%d").date() - start).days
        # Get summ of all payouts fot current purchase
        query = "select sum(Amount) from Payouts where ISIN = ? and Date > ?;"
        self.cursor.execute(query, (self.current_data['ISIN'], self.current_data['Purchase_date']))
        payouts = self.cursor.fetchone()[0]
        payouts = float(payouts) if payouts is not None else 0
        # Calculating net income
        net_income = round(self.current_data['Quantity'] * (1000 + payouts) - total, 2)
        # Now can calculate income % and year yield in %
        income_percent = round(net_income/total * 100, 2)
        year_yield = round(income_percent*365/period, 2)
        # Finally data ready for record in db
        query = "insert into Analytics (Total_purchase, Holding_period, Net_income, Income_percent, Year_yield_percent) values (?, ?, ?, ?, ?);"
        self.cursor.execute(query, (total, period, net_income, income_percent, year_yield))
        self.check_status()
        self.conn.commit()
        print(f"{datetime.now().time()}    Add row for purchase from {self.current_data['Purchase_date']}.")

    def check_status(self) -> None:
        """Checking purchase bond for paid off. Put 'Paid off' if yes."""
        query = """
        update Analytics 
        set Status = 'Paid off' 
        where exists (
            select 1
            from Bonds_info
            join Purchase_info on Bonds_info.ISIN = Purchase_info.ISIN
            where Analytics.ID = Purchase_info.ID
            and date('now') >= Bonds_info.Maturity_date
            );"""
        self.cursor.execute(query)
        print(f"{datetime.now().time()}    Status changed for {self.cursor.rowcount} transaction(s).")
        self.conn.commit()

    def add_exchange_date(self, new_date) -> None:
        """Add unique date to the table."""
        query = "insert or ignore into Exchange_rate (Date) values (?);"
        self.cursor.execute(query, (new_date,))
        if self.cursor.rowcount > 0:
            print(f"{datetime.now().time()}         New date({new_date}) was added.")
        self.conn.commit()

    def add_exchange_rate(self) -> None:
        """Add rates if it's available."""
        query = "select Date from Exchange_rate where (Rate_USD = 0.0 or Rate_EUR = 0.0) and date('now') > Date;"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        if result:
            for row_date in result:
                if self.count % 9 == 0:
                    print("Need some rest...offfff")
                    sleep(60)
                    print("Ok. Let's continue =)")
                self.count += 1
                print(f"{datetime.now().time()}         Looking for rates for {row_date}")
                usd, eur = get_exchange_rate(row_date)                
                query = """
                update Exchange_rate 
                set Rate_USD = ?,
                    Rate_EUR = ?
                where Date = ? and (Rate_USD = 0.0 or Rate_EUR = 0.0);
                """
                row_date = datetime.strptime(row_date[0], '%Y-%m-%d').date()
                self.cursor.execute(query, (usd, eur, row_date))
                self.conn.commit()
                print(f"{datetime.now().time()}         Update rates for {row_date}")
        else:
            print(f"{datetime.now().time()}     No date for update rate.")

    def close_connection(self) -> None:
        """Close connection."""
        self.cursor.close()
        self.conn.close()
    