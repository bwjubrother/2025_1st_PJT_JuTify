import yfinance as yf
import sqlite3

# 1. 주식 데이터 다운로드 함수
def download_stock_data(ticker, start_date, end_date):
    data = yf.download(ticker, start=start_date, end=end_date)
    data.reset_index(inplace=True)
    return data

# 2. 데이터베이스 연결 및 테이블 생성 함수
def initialize_database(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume INTEGER
        )
    ''')
    conn.commit()
    return conn

# 3. 데이터베이스에 데이터 삽입 함수
def insert_data_to_db(conn, data):
    cursor = conn.cursor()
    for _, row in data.iterrows():
        cursor.execute('''
            INSERT INTO stock_data (date, open, high, low, close, adj_close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (str(row['Date']), row['Open'], row['High'], row['Low'], row['Close'], row['Adj Close'], row['Volume']))
    conn.commit()

# 4. 데이터베이스에서 데이터 조회 함수
def fetch_data_from_db(conn, start_date=None, end_date=None):
    cursor = conn.cursor()
    query = "SELECT * FROM stock_data"
    params = []

    if start_date and end_date:
        query += " WHERE date BETWEEN ? AND ?"
        params.extend([start_date, end_date])

    cursor.execute(query, params)
    rows = cursor.fetchall()
    for row in rows:
        print(row)

# 5. 메인 실행 함수
def main():
    ticker = input("Enter the stock ticker (e.g., AAPL, MSFT): ")
    start_date = input("Enter the start date (YYYY-MM-DD): ")
    end_date = input("Enter the end date (YYYY-MM-DD): ")
    db_name = "stock_data.db"

    print("Downloading stock data...")
    stock_data = download_stock_data(ticker, start_date, end_date)

    print("Initializing database...")
    conn = initialize_database(db_name)

    print("Inserting data into the database...")
    insert_data_to_db(conn, stock_data)

    print("Data has been successfully stored in the database.")

    # 데이터 조회
    print("Fetching data from the database...")
    fetch_start_date = input("Enter the fetch start date (YYYY-MM-DD) or leave empty: ")
    fetch_end_date = input("Enter the fetch end date (YYYY-MM-DD) or leave empty: ")

    if fetch_start_date and fetch_end_date:
        fetch_data_from_db(conn, fetch_start_date, fetch_end_date)
    else:
        fetch_data_from_db(conn)

    conn.close()

if __name__ == "__main__":
    main()
