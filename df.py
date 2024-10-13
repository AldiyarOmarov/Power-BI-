import pandas as pd
import random
from faker import Faker
from datetime import datetime
from sqlalchemy import create_engine, text

fake = Faker('ru_RU')

num_rows = 100000
start_date = datetime(2021, 1, 1)
end_date = datetime(2023, 12, 1)

managers = [fake.name() for _ in range(5)]

data = {
    'номер_договора': [fake.unique.random_number(digits=6, fix_len=True) for _ in range(num_rows)],
    'период': [fake.date_between(start_date=start_date, end_date=end_date) for _ in range(num_rows)],
    'вид_тип_контракта': [random.choice(['машина', 'груз', 'имущество', 'жизнь', 'здоровье']) for _ in range(num_rows)],
    'менеджер': [random.choice(managers) for _ in range(num_rows)],
    'страховая_сумма': [round(random.uniform(10000, 1000000), 2) for _ in range(num_rows)]
}

df = pd.DataFrame(data)

df['номер_договора'] = df['номер_договора'].astype(int)

df.to_csv('contracts.csv', index=False, encoding='utf-8-sig', errors='replace')
print("Данные сохранены в contracts.csv")

engine = create_engine('postgresql://postgres:almaly@localhost:5432/postgres')

def create_table_structure(engine):
    with engine.connect() as con:
        create_table_sql = text("""
            CREATE TABLE IF NOT EXISTS contracts (
                номер_договора INTEGER PRIMARY KEY,
                период DATE,
                вид_тип_контракта VARCHAR(20),
                менеджер VARCHAR(100),
                страховая_сумма NUMERIC(15, 2)
            );
        """)
        con.execute(create_table_sql)
    print("Таблица contracts создана")

create_table_structure(engine)

try:
    df.to_sql('contracts', engine, index=False, if_exists='append', method='multi')
    print("Данные загружены в PostgreSQL")
except Exception as e:
    print(f"Ошибка: {e}")
