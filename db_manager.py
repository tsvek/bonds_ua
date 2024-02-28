import sqlite3

class DatabaseManager:

    def __init__(self, db_name='bonds_ua.db') -> None:
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()

    def create_tables(self):
        self.cursor.execute("""
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
        self.cursor.execute("""
            create table if not exists Bonds_info(
                ISIN char(12) PRIMARY KEY, 
                Nominal_yield numeric(10, 2) not null,
                Maturity_day date not null,
                FOREIGN KEY (ISIN) references Purchase_info (ISIN)
            );
        """)
        self.cursor.execute("""
            create table if not exists Payouts(
                ID int PRIMARY KEY,
                ISIN char(12) not null,
                Date date not null,
                Amount numeric(10, 2) not null,
                FOREIGN KEY (ISIN) references Bond_info (ISIN)
            );
        """)
        self.cursor.execute("""
            create table if not exists Exchange_rate(
                ID int PRIMARY KEY,
                Date date not null,
                Rate_USD numeric(10, 2),
                Rate_EUR numeric(10, 2)
            );
        """)
        self.cursor.execute("""
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
        self.cursor.execute("""
            create table if not exists Income_by_year(
                ID int PRIMARY KEY,
                ISIN char(12) not null,
                FOREIGN KEY (ISIN) references Purchase_info (ISIN) 
            );
        """)
        
    def add_purchase_row(self, info_list):
        query = "insert into Purchase_info (ISIN, Purchase_date, Quantity, Price, Reinvest, Broker, Fee) values (?, ?, ?, ?, ?, ?, ?);"
        self.cursor.execute(query, info_list)
        self.conn.commit()

    def close_connection(self):
        self.cursor.close()
        self.conn.close()


db = DatabaseManager()
db.create_tables()
db.close_connection()

    