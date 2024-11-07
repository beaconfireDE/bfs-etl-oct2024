import csv
import random
from datetime import datetime, timedelta


# Function to generate a CSV file
def generate_csv(filename, date):
    columns = ["stock_id", "date", "open_price", "close_price", "high_price", "low_price", "volume", "company_name",
               "sector", "market_cap"]
    data = []

    for i in range(1, 21):  # Generate 20 rows of data
        stock_id = f"S{i}"
        open_price = round(random.uniform(100, 200), 2)
        close_price = round(random.uniform(100, 200), 2)
        high_price = max(open_price, close_price) + round(random.uniform(0, 5), 2)
        low_price = min(open_price, close_price) - round(random.uniform(0, 5), 2)
        volume = random.randint(100000, 500000)
        company_name = f"Company_{i}"
        sector = random.choice(["Technology", "Healthcare", "Finance", "Energy", "Consumer"])
        market_cap = round(random.uniform(1, 100), 2) * 1e9

        data.append(
            [stock_id, date, open_price, close_price, high_price, low_price, volume, company_name, sector, market_cap])

    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(data)
    print(f"File {filename} has been generated.")


# Generate files for three days
base_date = datetime(2024, 11, 6)
for i in range(3):
    current_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
    filename = f"ThreeDaysData_Group5_{current_date}.csv"
    generate_csv(filename, current_date)
