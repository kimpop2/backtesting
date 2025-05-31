# backtesting/main.py

import logging
import sys
import os
from datetime import datetime, date, timedelta

# 프로젝트 루트 디렉토리를 Python path에 추가
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# 모듈 임포트
from db.db_manager import DBManager
from api_client.creon_api import CreonAPIClient
from data_manager.stock_data_manager import StockDataManager

# 로깅 설정 (Task 1에서 설정된 것 유지)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.StreamHandler(sys.stdout)
                    ])
logger = logging.getLogger(__name__)

def main():
    logger.info("백테스팅 시스템을 시작합니다.")

    # 1. DBManager 초기화 및 테이블 생성
    db_manager = DBManager()
    # 데이터베이스와 테이블이 없을 경우를 대비하여 생성
    # db_manager.drop_all_tables() # 테스트를 위해 기존 테이블을 삭제하고 싶을 경우 주석 해제
    db_manager.create_all_tables()

    # 2. CreonAPIClient 초기화
    creon_api_client = CreonAPIClient()
    if not creon_api_client.connected:
        logger.error("Creon Plus HTS에 연결할 수 없습니다. 프로그램을 종료합니다.")
        sys.exit(1) # HTS 연결 실패 시 종료

    # 3. StockDataManager 초기화
    stock_data_manager = StockDataManager(db_manager, creon_api_client)

    # 4. 초기 데이터 수집 (테스트 목적)
    logger.info("초기 데이터 수집을 시작합니다.")

    # 4.1. 모든 종목 정보 업데이트
    logger.info("모든 종목 정보 업데이트 중...")
    stock_data_manager.update_all_stock_info()
    logger.info("모든 종목 정보 업데이트 완료.")

    # 테스트할 특정 종목 코드
    test_stock_code = 'A005930' # 삼성전자
    test_stock_name = creon_api_client.get_stock_name(test_stock_code)
    if not test_stock_name:
        logger.warning(f"테스트 종목 ({test_stock_code})을 Creon API에서 찾을 수 없습니다. 다른 종목 코드를 시도하거나 HTS 상태를 확인하세요.")
        # 찾을 수 없으면 다른 종목으로 대체하거나 종료
        # 예: 목록에서 첫번째 종목으로 대체
        filtered_stocks = creon_api_client.get_filtered_stock_list()
        if filtered_stocks:
            test_stock_code = filtered_stocks[0]
            test_stock_name = creon_api_client.get_stock_name(test_stock_code)
            logger.info(f"테스트 종목을 {test_stock_name}({test_stock_code})으로 변경합니다.")
        else:
            logger.error("테스트할 종목이 없어 데이터 수집을 진행할 수 없습니다.")
            db_manager.close()
            sys.exit(1)


    # 4.2. 특정 종목의 일봉 데이터 업데이트
    logger.info(f"{test_stock_name}({test_stock_code}) 일봉 데이터 업데이트 중...")
    # 처음 데이터를 가져올 때는 넉넉한 기간을 지정 (예: 10년치)
    # 이미 데이터가 있다면 StockDataManager 내부 로직에서 최신 날짜 이후만 가져옴
    ten_years_ago = date.today() - timedelta(days=365 * 10)
    stock_data_manager.update_daily_ohlcv(test_stock_code, start_date=ten_years_ago)
    logger.info(f"{test_stock_name}({test_stock_code}) 일봉 데이터 업데이트 완료.")


    # 4.3. 특정 종목의 분봉 데이터 업데이트 (최근 일주일치)
    logger.info(f"{test_stock_name}({test_stock_code}) 분봉 데이터 업데이트 중 (최근 일주일)...")
    seven_days_ago = datetime.now() - timedelta(days=7)
    stock_data_manager.update_minute_ohlcv(test_stock_code, start_datetime=seven_days_ago, interval=1)
    logger.info(f"{test_stock_name}({test_stock_code}) 분봉 데이터 업데이트 완료.")

    # 4.4. 특정 종목의 재무 데이터 업데이트 (stock_info 테이블에 통합)
    logger.info(f"{test_stock_name}({test_stock_code}) stock_info 테이블의 최신 재무 데이터 업데이트 중...")
    stock_data_manager.update_financial_data_for_stock_info(test_stock_code)
    logger.info(f"{test_stock_name}({test_stock_code}) stock_info 테이블의 최신 재무 데이터 업데이트 완료.")
    # 5. DB 연결 종료
    db_manager.close()

    logger.info("백테스팅 시스템 초기 데이터 수집 및 테스트 완료.")

if __name__ == "__main__":
    main()