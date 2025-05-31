# backtesting/feeds/db_data_loader.py

import logging
import pandas as pd
from datetime import datetime, date
import backtrader as bt

import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from db.db_manager import DBManager

logger = logging.getLogger(__name__)

class DBDataLoader:
    """
    MariaDB에서 주식 데이터를 로드하여 backtrader의 PandasData 객체로 변환하는 클래스.
    """
    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager

    def load_daily_data(self, stock_code: str, fromdate: date, todate: date) -> bt.feeds.PandasData:
        """
        데이터베이스에서 특정 종목의 일봉 데이터를 로드하여 PandasData 객체로 반환합니다.
        :param stock_code: 종목 코드
        :param fromdate: 시작 날짜 (datetime.date)
        :param todate: 종료 날짜 (datetime.date)
        :return: backtrader.feeds.PandasData 인스턴스
        """
        # db_manager의 fetch_daily_data가 date 객체를 직접 받을 수 있도록
        # fromdate, todate를 그대로 전달합니다. (db_manager.py에 맞게 수정)
        logger.info(f"DB에서 {stock_code}의 일봉 데이터 로드 중: {fromdate} ~ {todate}")
        
        # DBManager에서 이미 DataFrame을 반환하므로 바로 사용
        # db_manager.py의 fetch_daily_data는 이미 datetime 객체로 변환된 'date' 컬럼을 반환합니다.
        df = self.db_manager.fetch_daily_data(
            stock_code=stock_code,
            start_date=fromdate, # db_manager.py에 정의된 파라미터 타입에 맞춤
            end_date=todate       # db_manager.py에 정의된 파라미터 타입에 맞춤
        )

        if df.empty:
            logger.warning(f"DB에 {stock_code}의 일봉 데이터가 없습니다. (기간: {fromdate} ~ {todate})")
            # DBManager가 컬럼을 가진 빈 DataFrame을 반환하므로, 컬럼 이름 변경만 하면 됩니다.
            # 'date' 컬럼은 이미 datetime으로 변환된 상태입니다.
            df.rename(columns={
                'date': 'datetime', # backtrader의 기본 datetime 컬럼명으로 변경
                'open_price': 'open',
                'high_price': 'high',
                'low_price': 'low',
                'close_price': 'close',
                'volume': 'volume'
            }, inplace=True)
            # 빈 DataFrame이라도 인덱스 설정은 일관성을 위해 유지
            if 'datetime' in df.columns: # 'date'가 'datetime'으로 변경되었으므로 확인
                df = df.set_index('datetime')
            
            # 여기서 PandasData의 'datetime' 파라미터는 DataFrame의 인덱스 이름(또는 컬럼 이름)을 지정합니다.
            return bt.feeds.PandasData(dataname=df, fromdate=fromdate, todate=todate, datetime='datetime')

        # 데이터프레임의 컬럼명을 backtrader가 인식하는 이름으로 변경
        # 'date' 컬럼을 'datetime'으로 변경하고 인덱스로 설정
        df.rename(columns={
            'date': 'datetime', # 'date' 컬럼을 backtrader의 기본 datetime 컬럼명으로 변경
            'open_price': 'open',
            'high_price': 'high',
            'low_price': 'low',
            'close_price': 'close',
            'volume': 'volume'
        }, inplace=True)

        df = df.set_index('datetime') # 'datetime' 컬럼을 인덱스로 설정
        df = df.sort_index()          # 시간 순서대로 정렬

        df = df[['open', 'high', 'low', 'close', 'volume']] # backtrader에 필요한 OHLCV 컬럼만 선택
        
        logger.info(f"{stock_code} 일봉 데이터 {len(df)}개 로드 완료.")
        # PandasData의 datetime 파라미터는 인덱스 이름이므로 'datetime'으로 지정
        return bt.feeds.PandasData(dataname=df, fromdate=fromdate, todate=todate, datetime='datetime')

    def load_minute_data(self, stock_code: str, fromdatetime: datetime, todatetime: datetime) -> bt.feeds.PandasData:
        """
        데이터베이스에서 특정 종목의 분봉 데이터를 로드하여 PandasData 객체로 반환합니다.
        :param stock_code: 종목 코드
        :param fromdatetime: 시작 날짜/시간 (datetime.datetime)
        :param todatetime: 종료 날짜/시간 (datetime.datetime)
        :return: backtrader.feeds.PandasData 인스턴스
        """
        # db_manager의 fetch_minute_data가 datetime 객체를 직접 받을 수 있도록
        # fromdatetime, todatetime을 그대로 전달합니다.
        logger.info(f"DB에서 {stock_code}의 분봉 데이터 로드 중: {fromdatetime} ~ {todatetime}")
        df = self.db_manager.fetch_minute_data(
            stock_code=stock_code,
            start_datetime=fromdatetime, # db_manager.py에 정의된 파라미터 타입에 맞춤
            end_datetime=todatetime       # db_manager.py에 정의된 파라미터 타입에 맞춤
        )

        if df.empty:
            logger.warning(f"DB에 {stock_code}의 분봉 데이터가 없습니다. (기간: {fromdatetime} ~ {todatetime})")
            # DBManager가 컬럼을 가진 빈 DataFrame을 반환하므로, 컬럼 이름 변경만 하면 됩니다.
            # 'datetime' 컬럼은 이미 datetime으로 변환된 상태입니다.
            df.rename(columns={
                'open_price': 'open',
                'high_price': 'high',
                'low_price': 'low',
                'close_price': 'close',
                'volume': 'volume'
            }, inplace=True)
            if 'datetime' in df.columns: # 이미 'datetime' 컬럼이 존재하므로 확인
                df = df.set_index('datetime')

            return bt.feeds.PandasData(dataname=df, fromdate=fromdatetime, todate=todatetime, datetime='datetime')

        df.rename(columns={
            'open_price': 'open',
            'high_price': 'high',
            'low_price': 'low',
            'close_price': 'close',
            'volume': 'volume'
        }, inplace=True)
        
        df = df.set_index('datetime') # 'datetime' 컬럼을 인덱스로 설정
        df = df.sort_index()          # 시간 순서대로 정렬

        df = df[['open', 'high', 'low', 'close', 'volume']]
        
        logger.info(f"{stock_code} 분봉 데이터 {len(df)}개 로드 완료.")
        return bt.feeds.PandasData(dataname=df, fromdate=fromdatetime, todate=todatetime, datetime='datetime')