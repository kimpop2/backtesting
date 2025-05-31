# backtesting/data_manager/stock_data_manager.py

import logging
import pandas as pd
from datetime import datetime, timedelta, date
import os
import sys

# sys.path에 프로젝트 루트 추가 (모듈 임포트를 위함)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from db.db_manager import DBManager
from api_client.creon_api import CreonAPIClient
# from config.settings import DEFAULT_OHLCV_DAYS_TO_FETCH # 향후 사용될 수 있음

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class StockDataManager:
    def __init__(self, db_manager: DBManager, creon_api_client: CreonAPIClient):
        self.db_manager = db_manager
        self.creon_api_client = creon_api_client
        logger.info("StockDataManager 초기화 완료.")

    def update_all_stock_info(self):
        """
        Creon API에서 모든 종목 정보를 가져와 DB에 저장/업데이트합니다.
        """
        logger.info("모든 종목 정보 업데이트를 시작합니다.")
        if not self.creon_api_client.connected:
            logger.error("Creon API가 연결되어 있지 않아 종목 정보를 가져올 수 없습니다.")
            return False

        try:
            # CreonAPIClient의 내부 딕셔너리에서 필터링된 종목 정보 가져오기
            filtered_codes = self.creon_api_client.get_filtered_stock_list()
            stock_info_list = []
            for code in filtered_codes:
                name = self.creon_api_client.get_stock_name(code)
                # CreonAPIClient의 _make_stock_dic에서 이미 기본 필터링 완료
                # 추가적인 시장 구분, 섹터, PER, PBR, EPS는 Creon CpUtil.CpCodeMgr에서 제공되지만
                # 현재 StockInfo 테이블의 PER, PBR, EPS는 CpSvr7254 (재무 데이터)에서 가져오는 것이 일반적.
                # 여기서는 stock_name, stock_code, market_type만 먼저 채우고,
                # 나머지 필드는 나중에 재무 데이터 업데이트 시 채울 예정.
                market_type = 'KOSPI' if code in self.creon_api_client.cp_code_mgr.GetStockListByMarket(1) else 'KOSDAQ'

                stock_info_list.append({
                    'stock_code': code,
                    'stock_name': name,
                    'market_type': market_type,
                    'sector': None, # 추후 재무 데이터나 다른 API로 보완
                    'per': None,
                    'pbr': None,
                    'eps': None
                })

            if stock_info_list:
                if self.db_manager.save_stock_info(stock_info_list):
                    logger.info(f"{len(stock_info_list)}개의 종목 정보를 성공적으로 DB에 업데이트했습니다.")
                    return True
                else:
                    logger.error("종목 정보 DB 저장에 실패했습니다.")
                    return False
            else:
                logger.warning("가져올 종목 정보가 없습니다. Creon HTS 연결 상태 및 종목 필터링 조건을 확인하세요.")
                return False
        except Exception as e:
            logger.error(f"모든 종목 정보 업데이트 중 오류 발생: {e}", exc_info=True)
            return False

    def update_daily_ohlcv(self, stock_code, start_date=None, end_date=None):
        """
        특정 종목의 일봉 데이터를 Creon API에서 가져와 DB에 저장/업데이트합니다.
        기존 DB에 데이터가 있다면 최신 날짜 이후의 데이터만 가져와서 추가합니다.
        :param stock_code: 종목 코드
        :param start_date: 조회 시작 날짜 (datetime.date 객체). None이면 DB 최신 날짜 + 1일 부터 조회.
        :param end_date: 조회 종료 날짜 (datetime.date 객체). None이면 오늘 날짜까지 조회.
        """
        logger.info(f"{stock_code} 일봉 데이터 업데이트를 시작합니다.")

        if not self.creon_api_client.connected:
            logger.error("Creon API가 연결되어 있지 않아 일봉 데이터를 가져올 수 없습니다.")
            return False

        if not end_date:
            end_date = date.today()

        db_latest_date = self.db_manager.get_latest_daily_data_date(stock_code)

        fetch_start_date = start_date
        if db_latest_date:
            # DB에 이미 데이터가 있다면, 최신 날짜 다음 날부터 가져옵니다.
            if fetch_start_date: # 시작 날짜가 지정된 경우
                fetch_start_date = max(fetch_start_date, db_latest_date + timedelta(days=1))
            else: # 시작 날짜가 지정되지 않은 경우
                fetch_start_date = db_latest_date + timedelta(days=1)
        elif not fetch_start_date:
            # DB에 데이터가 없고, 시작 날짜도 지정되지 않았다면, 1년 전부터 가져옵니다.
            # settings.py의 DEFAULT_OHLCV_DAYS_TO_FETCH 사용 가능
            fetch_start_date = end_date - timedelta(days=365 * 5) # 기본 5년치

        if fetch_start_date > end_date:
            logger.info(f"{stock_code} 일봉 데이터는 최신 상태입니다. 업데이트할 데이터가 없습니다.")
            return True

        start_date_str = fetch_start_date.strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')

        ohlcv_df = self.creon_api_client.get_daily_ohlcv(stock_code, start_date_str, end_date_str)

        if ohlcv_df.empty:
            logger.info(f"{stock_code} 기간 {start_date_str}~{end_date_str} 동안 Creon API에서 조회된 일봉 데이터가 없습니다.")
            return True

        # 등락률(change_rate) 계산
        # DB에서 이전 종가를 가져와서 계산하거나, 조회된 데이터프레임 내에서 계산
        ohlcv_df = ohlcv_df.sort_values(by='date', ascending=True).reset_index(drop=True)
        ohlcv_df['prev_close_price'] = ohlcv_df['close_price'].shift(1)
        ohlcv_df['change_rate'] = ((ohlcv_df['close_price'] - ohlcv_df['prev_close_price']) / ohlcv_df['prev_close_price'] * 100).round(2)
        ohlcv_df.loc[0, 'change_rate'] = 0.0 # 첫 데이터의 등락률은 0으로 설정하거나 별도 처리

        # 필요한 컬럼만 선택하여 DB에 저장할 형태로 변환
        save_data = ohlcv_df[['stock_code', 'date', 'open_price', 'high_price',
                            'low_price', 'close_price', 'volume', 'change_rate', 'trading_value']].to_dict(orient='records')

        if save_data:
            if self.db_manager.save_daily_data(save_data):
                logger.info(f"{stock_code} 일봉 데이터 {len(save_data)}개를 성공적으로 DB에 업데이트했습니다.")
                return True
            else:
                logger.error(f"{stock_code} 일봉 데이터 DB 저장에 실패했습니다.")
                return False
        else:
            logger.info(f"{stock_code} 업데이트할 새로운 일봉 데이터가 없습니다.")
            return True

    def update_minute_ohlcv(self, stock_code, start_datetime=None, end_datetime=None, interval=1):
        """
        특정 종목의 분봉 데이터를 Creon API에서 가져와 DB에 저장/업데이트합니다.
        기존 DB에 데이터가 있다면 최신 시각 이후의 데이터만 가져와서 추가합니다.
        :param stock_code: 종목 코드
        :param start_datetime: 조회 시작 시각 (datetime.datetime 객체). None이면 DB 최신 시각 + 1분 부터 조회.
        :param end_datetime: 조회 종료 시각 (datetime.datetime 객체). None이면 현재 시각까지 조회.
        :param interval: 분봉 주기 (기본 1분)
        """
        logger.info(f"{stock_code} {interval}분봉 데이터 업데이트를 시작합니다.")

        if not self.creon_api_client.connected:
            logger.error("Creon API가 연결되어 있지 않아 분봉 데이터를 가져올 수 없습니다.")
            return False

        if not end_datetime:
            end_datetime = datetime.now()

        db_latest_datetime = self.db_manager.get_latest_minute_data_datetime(stock_code)

        fetch_start_datetime = start_datetime
        if db_latest_datetime:
            # DB에 이미 데이터가 있다면, 최신 시각 다음 분부터 가져옵니다.
            if fetch_start_datetime: # 시작 시각이 지정된 경우
                fetch_start_datetime = max(fetch_start_datetime, db_latest_datetime + timedelta(minutes=interval))
            else: # 시작 시각이 지정되지 않은 경우
                fetch_start_datetime = db_latest_datetime + timedelta(minutes=interval)
        elif not fetch_start_datetime:
            # DB에 데이터가 없고, 시작 시각도 지정되지 않았다면, 최근 며칠치만 가져옵니다.
            # settings.py의 DEFAULT_MINUTE_DAYS_TO_FETCH 사용 가능
            fetch_start_datetime = end_datetime - timedelta(days=7) # 기본 7일치

        if fetch_start_datetime > end_datetime:
            logger.info(f"{stock_code} 분봉 데이터는 최신 상태입니다. 업데이트할 데이터가 없습니다.")
            return True

        start_date_str = fetch_start_datetime.strftime('%Y%m%d')
        end_date_str = end_datetime.strftime('%Y%m%d')

        ohlcv_df = self.creon_api_client.get_minute_ohlcv(stock_code, start_date_str, end_date_str, interval)

        if ohlcv_df.empty:
            logger.info(f"{stock_code} 기간 {start_date_str}~{end_date_str} 동안 Creon API에서 조회된 분봉 데이터가 없습니다.")
            return True

        # 필요한 컬럼만 선택하여 DB에 저장할 형태로 변환
        save_data = ohlcv_df[['stock_code', 'datetime', 'open_price', 'high_price',
                            'low_price', 'close_price', 'volume']].to_dict(orient='records')

        if save_data:
            if self.db_manager.save_minute_data(save_data):
                logger.info(f"{stock_code} 분봉 데이터 {len(save_data)}개를 성공적으로 DB에 업데이트했습니다.")
                return True
            else:
                logger.error(f"{stock_code} 분봉 데이터 DB 저장에 실패했습니다.")
                return False
        else:
            logger.info(f"{stock_code} 업데이트할 새로운 분봉 데이터가 없습니다.")
            return True

    def update_finance_data(self, stock_code, period_type='annual', count=5):
        """
        특정 종목의 재무 데이터를 Creon API에서 가져와 DB에 저장/업데이트합니다.
        :param stock_code: 종목 코드
        :param period_type: 'annual' (연간) 또는 'quarter' (분기별)
        :param count: 조회할 기간 개수 (최근 N년/분기)
        """
        logger.info(f"{stock_code} 재무 데이터 ({period_type}, 최근 {count}개) 업데이트를 시작합니다.")

        if not self.creon_api_client.connected:
            logger.error("Creon API가 연결되어 있지 않아 재무 데이터를 가져올 수 없습니다.")
            return False

        finance_df = self.creon_api_client.get_financial_data(stock_code, period_type, count)

        if finance_df.empty:
            logger.info(f"{stock_code} Creon API에서 조회된 재무 데이터가 없습니다.")
            return True

        # 필요한 컬럼만 선택하여 DB에 저장할 형태로 변환
        save_data = finance_df[['stock_code', 'base_date', 'quarter', 'sales', 'operating_profit',
                                'net_profit', 'per', 'pbr', 'roe', 'debt_ratio']].to_dict(orient='records')

        if save_data:
            if self.db_manager.save_finance_data(save_data):
                logger.info(f"{stock_code} 재무 데이터 {len(save_data)}개를 성공적으로 DB에 업데이트했습니다.")
                return True
            else:
                logger.error(f"{stock_code} 재무 데이터 DB 저장에 실패했습니다.")
                return False
        else:
            logger.info(f"{stock_code} 업데이트할 새로운 재무 데이터가 없습니다.")
            return True