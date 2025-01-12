import pandas as pd
import FinanceDataReader as fdr

def load_allstock_KRX():
    krx_url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
    stk_data = pd.read_html(krx_url, header=0)[0]
    stk_data = stk_data[['회사명', '종목코드']]
    stk_data = stk_data.rename(columns={'회사명': 'Name', '종목코드': 'Code'})

    stk_data['Code'] = stk_data['Code'].apply(lambda input: '0' * (6 - len(str(input))) + str(input))

    return stk_data

if __name__ == "__main__":
    df_stock = load_allstock_KRX()
    print(df_stock)         # 위의 5행 print