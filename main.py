import re

from datetime import datetime
from db_manager import DatabaseManager

def input_data() -> dict:
    """Enter the purchase info. Return dict."""
    input_info = {
        'ISIN': input("Enter bond's code(UAxxxxxxxxxx): ").upper(),
        'Purchase_date': datetime.strptime(input("Enter date of purchase(dd.mm.YYYY): "), '%d.%m.%Y').date(), 
        'Quantity': int(input("Enter the number of bond purchased: ")),
        'Price': float(input("Enter the bond's price: ")),
        'Reinvest': float(input("Enter the amount of re-investment: ")),
        'Broker': input("Enter the broker name: "),
        'Fee': float(input("Enter the fee: ")),
    }
    return input_info

def check_inputs(inputs):
    """Check inputs for valid format."""
    if not re.match(r"UA\d{10}", inputs['ISIN']):
        return "Enter valid ISIN!"
    try:
        purchase_date = datetime.strptime(inputs['Purchase_date'], '%d.%m.%Y').date()
    except ValueError:
        return "Enter valid date format!"
    if date.today() <= purchase_date:
        return "Enter valid purchase date!"
    if not inputs["Quantity"].isdigit():
        return "Enter quantity in digits!"
    try:
        float(inputs["Price"])
        float(inputs["Reinvest"])
        float(inputs["Fee"])
    except ValueError:
        return "Enter valid number!"
    return True

dm = DatabaseManager()
print("Let's add new purchase...")
new_purchase = input_data()
dm.add_purchase_row(new_purchase)
print("New purchase and information added.")
dm.close_connection()
