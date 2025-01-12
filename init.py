import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm  # 진행 바 표시
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor 

# 1. KRX 에서 종목 코드 가져오기
def get_krx_stock_codes():
    krx_url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
    stk_data = pd.read_html(krx_url, header=0)[0]
    stk_data = stk_data[['회사명', '종목코드']]
    stk_data = stk_data.rename(columns={'회사명': 'Name', '종목코드': 'Code'})

    stk_data['Code'] = stk_data['Code'].apply(lambda input: '0' * (6 - len(str(input))) + str(input))
    stk_data = stk_data.to_dict('records')

    return stk_data

# 2. 각 종목 데이터 다운로드 및 변동성 계산
def calculate_volatility_for_all(symbols):
    results = []
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365)

    # for symbol in tqdm(symbols, desc="종목 처리 중"): # 진행 경과율 출력하고 싶을 때
    for symbol in symbols:
        # yfinance용 종목 코드 변환 (KRX -> yfinance 포맷)
        if symbol['Code'].startswith("0") or symbol['Code'].startswith("1"):  # 코스닥
            code = f"{symbol['Code']}.KQ"
        else:  # 코스피
            code = f"{symbol['Code']}.KS"

        # code = symbol['Code']
        name = symbol['Name']
        print(code, name)
        try:
            # 주가 데이터 다운로드
            data = yf.download(code, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
            
            # 데이터가 없으면 건너뜀
            if data.empty:
                continue
            
            # 수익률 계산
            data['Return'] = data['Adj Close'].pct_change()
            
            # 변동성 계산
            daily_volatility = data['Return'].std()
            annual_volatility = daily_volatility * (252 ** 0.5)
            
            # 결과 저장
            results.append({
                "종목 코드": code,
                "종목명": name,
                "일간 변동성": daily_volatility,
                "연간 변동성": annual_volatility
            })
        except Exception as e:
            print(f"종목 {name}({code}) 처리 중 오류 발생: {e}")
    
    return pd.DataFrame(results)

# 3. 결과 실행
if __name__ == "__main__":
    krx_stocks = get_krx_stock_codes()  # 종목 리스트 가져오기
    volatility_data = calculate_volatility_for_all(krx_stocks)  # 변동성 계산

    # 결과 출력
    print(volatility_data)
    
    # 결과 저장
    # volatility_data.to_csv("kospi_kosdaq_volatility.csv", index=False, encoding="utf-8-sig")
    # print("변동성 계산 완료! 결과가 CSV 파일로 저장되었습니다.")
