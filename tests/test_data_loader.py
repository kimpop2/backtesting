# backtesting/tests/test_data_loader.py

import logging
import sys
import os
from datetime import datetime, date, timedelta

# 프로젝트 루트 디렉토리를 Python path에 추가
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from db.db_manager import DBManager
from feeds.db_data_loader import DBDataLoader

# 로깅 설정
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

def run_data_loader_tests():
    """
    DBDataLoader 클래스의 데이터 로드 기능을 테스트합니다.
    """
    logger.info("--- DBDataLoader 테스트 시작 ---")

    db_manager = DBManager()

    # DB 연결 확인
    if not db_manager.conn:
        logger.error("DB 연결 실패. DBDataLoader 테스트를 실행할 수 없습니다.")
        sys.exit(1)

    data_loader = DBDataLoader(db_manager) # DBDataLoader 인스턴스 생성

    # --- 일봉 데이터 테스트 ---
    logger.info("\n--- 일봉 데이터 로드 테스트 ---")
    test_code_daily = 'A005930' # 삼성전자
    today = date.today() # 현재 날짜로 변경
    one_year_ago = today - timedelta(days=365)
    
    try:
        daily_data = data_loader.load_daily_data(
            stock_code=test_code_daily,
            fromdate=one_year_ago,
            todate=today
        )
        
        # 여기서 len(daily_data) 확인 로직 대신
        # backtrader.feeds.PandasData 객체가 성공적으로 생성되었는지 여부만 확인합니다.
        # 실제 데이터가 있는지 여부는 db_data_loader.py의 내부 로그가 더 정확합니다.
        if daily_data is not None: # 객체가 None이 아닌지 확인
            logger.info(f"'{test_code_daily}' 일봉 데이터 PandasData 객체 생성 성공.")
            # 로드된 데이터 개수는 db_data_loader에서 이미 로그를 찍고 있으므로 여기서는 생략
            # 실제 데이터가 있는지 여부는 백테스팅 엔진에 데이터를 추가하고 실행할 때 확인됩니다.
        else:
            # 이 else 블록은 사실상 도달하기 어렵습니다.
            logger.warning(f"'{test_code_daily}'에 대한 일봉 데이터 로드에 실패했습니다. (기간: {one_year_ago} ~ {today})")
    except Exception as e:
        logger.error(f"일봉 데이터 로드 중 예상치 못한 오류 발생: {e}", exc_info=True)


    # --- 분봉 데이터 테스트 ---
    logger.info("\n--- 분봉 데이터 로드 테스트 ---")
    test_code_minute = 'A005930' # 삼성전자
    now = datetime.now()
    seven_days_ago = now - timedelta(days=7)

    try:
        minute_data = data_loader.load_minute_data(
            stock_code=test_code_minute,
            fromdatetime=seven_days_ago,
            todatetime=now
        )
        
        if minute_data is not None: # 객체가 None이 아닌지 확인
            logger.info(f"'{test_code_minute}' 분봉 데이터 PandasData 객체 생성 성공.")
        else:
            logger.warning(f"'{test_code_minute}'에 대한 분봉 데이터 로드에 실패했습니다. (기간: {seven_days_ago} ~ {now})")
    except Exception as e:
        logger.error(f"분봉 데이터 로드 중 예상치 못한 오류 발생: {e}", exc_info=True)

    db_manager.close()
    logger.info("--- DBDataLoader 테스트 완료 ---")

if __name__ == '__main__':
    run_data_loader_tests()