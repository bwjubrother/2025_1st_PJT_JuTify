# 2. 병렬로 데이터를 다운로드하는 함수
def download_all_stock_data(stock_codes):
    with ThreadPoolExecutor(max_workers=10) as executor:
        # 최대 10개의 작업을 병렬로 실행
        results = list(executor.map(download_stock_data, stock_codes))

    # 다운로드한 데이터 중 None이 아닌 값만 필터링
    valid_results = [result for result in results if result is not None]
    
    # 각 주식 데이터의 종목 코드 추가 (주식 데이터에 종목 코드 붙이기)
    for i, data in enumerate(valid_results):
        data['종목 코드'] = stock_codes[i]
    
    # 데이터프레임 합치기
    all_data = pd.concat(valid_results, ignore_index=True)
    
    return all_data

###############################################################

# 병렬로 실행할 함수
def worker_function(name):
    print(f"작업 시작: {name}")
    time.sleep(2)  # 작업 지연
    return f"작업 완료: {name}"

if __name__ == "__main__":
    # 작업 목록
    tasks = ["Task 1", "Task 2", "Task 3", "Task 4"]

    # ThreadPoolExecutor 사용
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(worker_function, tasks)

    for result in results:
        print(result)

    print("모든 작업 완료")