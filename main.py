# backtesting/main.py

import logging
import sys
import os

# 프로젝트 루트 디렉토리를 Python path에 추가
# 현재 스크립트(main.py)가 backtesting/ 디렉토리에 있으므로,
# os.path.dirname(__file__)은 'backtesting' 경로를 반환합니다.
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# 로깅 설정
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.StreamHandler(sys.stdout)
                    ])
logger = logging.getLogger(__name__)

def main():
    logger.info("백테스팅 시스템을 시작합니다.")
    # 여기에 향후 데이터 업데이트, 백테스팅 실행 등의 로직을 추가합니다.
    logger.info("백테스팅 시스템 초기화 완료.")

if __name__ == "__main__":
    main()