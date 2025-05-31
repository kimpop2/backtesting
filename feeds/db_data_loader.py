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
        logger.info(f"DB에서 {stock_code}의 일봉 데이터 로드 중: {fromdate} ~ {todate}")
        
        df = self.db_manager.fetch_daily_data(
            stock_code=stock_code,
            start_date=fromdate,
            end_date=todate
        )

        if df.empty:
            logger.warning(f"DB에 {stock_code}의 일봉 데이터가 없습니다. (기간: {fromdate} ~ {todate})")
            # 빈 DataFrame이라도 backtrader가 기대하는 컬럼과 인덱스 타입을 맞춥니다.
            # 'date'를 'datetime'으로 변경 (rename)
            # 'datetime'을 인덱스로 설정
            # 인덱스를 datetime.datetime 타입으로 강제 변환
            # (이 부분은 실제로 데이터가 없으면 빈 DataFrame에 적용되어도 오류는 안남)
            df_columns = ['open', 'high', 'low', 'close', 'volume']
            empty_df = pd.DataFrame(columns=df_columns)
            empty_df.index.name = 'datetime' # 인덱스 이름 설정
            empty_df.index = pd.to_datetime(empty_df.index) # 인덱스 타입을 datetime으로
            return bt.feeds.PandasData(dataname=empty_df, fromdate=fromdate, todate=todate)


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
        
        # !!!! 중요: 인덱스가 datetime.datetime 타입인지 확인하고 변환 (이번 오류 해결을 위해)
        # pd.to_datetime은 이미 datetime.datetime 객체인 경우 아무것도 하지 않고 반환합니다.
        # datetime.date 객체인 경우, 00:00:00 시간을 추가하여 datetime.datetime으로 변환합니다.
        df.index = pd.to_datetime(df.index) 
        
        df = df.sort_index()          # 시간 순서대로 정렬

        df = df[['open', 'high', 'low', 'close', 'volume']] # backtrader에 필요한 OHLCV 컬럼만 선택
        
        logger.info(f"{stock_code} 일봉 데이터 {len(df)}개 로드 완료.")
        return bt.feeds.PandasData(dataname=df, fromdate=fromdate, todate=todate)

    def load_minute_data(self, stock_code: str, fromdatetime: datetime, todatetime: datetime) -> bt.feeds.PandasData:
        """
        데이터베이스에서 특정 종목의 분봉 데이터를 로드하여 PandasData 객체로 반환합니다.
        :param stock_code: 종목 코드
        :param fromdatetime: 시작 날짜/시간 (datetime.datetime)
        :param todatetime: 종료 날짜/시간 (datetime.datetime)
        :return: backtrader.feeds.PandasData 인스턴스
        """
        logger.info(f"DB에서 {stock_code}의 분봉 데이터 로드 중: {fromdatetime} ~ {todatetime}")
        df = self.db_manager.fetch_minute_data(
            stock_code=stock_code,
            start_datetime=fromdatetime,
            end_datetime=todatetime
        )

        if df.empty:
            logger.warning(f"DB에 {stock_code}의 분봉 데이터가 없습니다. (기간: {fromdatetime} ~ {todatetime})")
            df_columns = ['open', 'high', 'low', 'close', 'volume']
            empty_df = pd.DataFrame(columns=df_columns)
            empty_df.index.name = 'datetime'
            empty_df.index = pd.to_datetime(empty_df.index) # 인덱스 타입을 datetime으로
            return bt.feeds.PandasData(dataname=empty_df, fromdate=fromdatetime, todate=todatetime)

        df.rename(columns={
            'open_price': 'open',
            'high_price': 'high',
            'low_price': 'low',
            'close_price': 'close',
            'volume': 'volume'
        }, inplace=True)
        
        df = df.set_index('datetime') # 'datetime' 컬럼을 인덱스로 설정
        
        # 분봉 데이터는 이미 datetime.datetime 타입일 가능성이 높지만, 혹시 모를 상황에 대비하여 추가
        df.index = pd.to_datetime(df.index) 
        
        df = df.sort_index()          # 시간 순서대로 정렬

        df = df[['open', 'high', 'low', 'close', 'volume']]
        
        logger.info(f"{stock_code} 분봉 데이터 {len(df)}개 로드 완료.")
        return bt.feeds.PandasData(dataname=df, fromdate=fromdatetime, todate=todatetime)