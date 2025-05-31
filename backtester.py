# backtesting/backtester.py

import backtrader as bt
from datetime import datetime, date, timedelta
import logging
import sys
import os

# 프로젝트 루트 디렉토리를 Python path에 추가
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from db.db_manager import DBManager
from feeds.db_data_loader import DBDataLoader

from strategies.simple_ma_strategy import SimpleMAStrategy

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # 기본 로그 레벨 설정

class Backtester:
    """
    backtrader Cerebro 엔진을 설정하고 백테스팅을 실행하는 클래스.
    """
    def __init__(self, start_date: date, end_date: date, cash: float = 100_000_000):
        """
        Backtester를 초기화합니다.
        :param start_date: 백테스팅 시작 날짜 (datetime.date 객체)
        :param end_date: 백테스팅 종료 날짜 (datetime.date 객체)
        :param cash: 초기 투자 자산
        """
        self.cerebro = bt.Cerebro()
        self.db_manager = DBManager()
        self.data_loader = DBDataLoader(self.db_manager)
        self.start_date = start_date
        self.end_date = end_date
        self.cash = cash
        self._setup_cerebro()

    def _setup_cerebro(self):
        """
        Cerebro 엔진의 초기 설정을 수행합니다.
        """
        # 1. 초기 자산 설정
        self.cerebro.broker.setcash(self.cash)
        logger.info(f"초기 자산 설정 완료: {self.cash:,.0f}원")

        # 2. 수수료 설정 (예시: 매수/매도 시 0.15% 수수료)
        # 실제 증권사 수수료와 슬리피지를 고려하여 설정해야 합니다.
        self.cerebro.broker.setcommission(commission=0.0015)
        logger.info("거래 수수료 설정 완료: 0.15%")

        # 3. 리샘플링 전략 및 기타 설정 (필요시 추가)
        # 예: 일봉 데이터를 사용하여 백테스팅하므로, 특별한 리샘플링은 필요 없을 수 있습니다.
        # self.cerebro.broker.set_cooldown(False) # 백테스팅 시 거래 간 쿨다운 해제 (선택 사항)

    def add_data(self, stock_code: str, timeframe='daily'):
        """
        Cerebro에 데이터를 추가합니다.
        :param stock_code: 종목 코드 (예: 'A005930')
        :param timeframe: 'daily' 또는 'minute' (현재는 daily만 지원)
        """
        logger.info(f"데이터 '{stock_code}' ({timeframe}) 로드 및 Cerebro에 추가 중...")
        try:
            if timeframe == 'daily':
                data = self.data_loader.load_daily_data(
                    stock_code=stock_code,
                    fromdate=self.start_date,
                    todate=self.end_date
                )
            elif timeframe == 'minute':
                # 분봉 데이터 로드 시 시작/종료 시간도 고려해야 함.
                # 현재는 테스트를 위해 전체 기간을 불러오지만, 실제 사용 시에는 특정 일자를 지정할 수 있습니다.
                # 예시를 위해 fromdatetime, todatetime을 인자로 받도록 수정 필요
                data = self.data_loader.load_minute_data(
                    stock_code=stock_code,
                    fromdatetime=datetime.combine(self.start_date, datetime.min.time()),
                    todatetime=datetime.combine(self.end_date, datetime.max.time())
                )
            else:
                raise ValueError("지원하지 않는 timeframe입니다. 'daily' 또는 'minute'을 사용하세요.")

            if data is not None:
                self.cerebro.adddata(data)
                logger.info(f"'{stock_code}' ({timeframe}) 데이터 Cerebro에 추가 완료.")
            else:
                logger.warning(f"'{stock_code}' ({timeframe}) 데이터 로드 실패. Cerebro에 추가하지 않습니다.")

        except Exception as e:
            logger.error(f"데이터 '{stock_code}' 로드 및 Cerebro 추가 중 오류 발생: {e}", exc_info=True)

    def add_strategy(self, strategy, *args, **kwargs):
        """
        Cerebro에 백테스팅 전략을 추가합니다.
        :param strategy: backtrader.Strategy 클래스
        :param args: 전략 초기화에 필요한 위치 인자
        :param kwargs: 전략 초기화에 필요한 키워드 인자
        """
        self.cerebro.addstrategy(strategy, *args, **kwargs)
        logger.info(f"전략 '{strategy.__name__}' Cerebro에 추가 완료.")

    def run(self):
        """
        백테스팅을 실행하고 결과를 반환합니다.
        """
        logger.info("백테스팅 시작...")
        if not self.cerebro.datas:
            logger.error("Cerebro에 추가된 데이터가 없습니다. run()을 실행하기 전에 add_data()를 호출하세요.")
            return None

        # Cerebro 실행
        # backtrader는 실행 후 결과를 반환하며, 이는 주로 cerebro.run()의 리턴값으로 처리됩니다.
        # 이 예제에서는 단순하게 runs를 반환합니다.
        try:
            # logstats=False 로 설정하면 백테스팅 중 출력되는 기본 통계 출력을 줄일 수 있습니다.
            # 하지만 상세한 로그를 원한다면 True로 설정합니다.
            strategies = self.cerebro.run(maxcpus=1) # 멀티코어 사용 시 maxcpus > 1, 아니면 1 또는 None
            logger.info("백테스팅 완료.")
            return strategies # 실행된 전략 인스턴스 리스트를 반환합니다.
        except Exception as e:
            logger.error(f"백테스팅 실행 중 오류 발생: {e}", exc_info=True)
            return None
        finally:
            self.db_manager.close() # 백테스팅 완료 후 DB 연결 종료

# 테스트를 위한 메인 실행 블록
if __name__ == '__main__':
    # TestStrategy 대신 SimpleMAStrategy를 직접 사용할 것이므로 이 클래스는 삭제하거나 주석 처리합니다.
    # class TestStrategy(bt.Strategy):
    #     def __init__(self):
    #         self.dataclose = self.datas[0].close
    #     def next(self):
    #         pass
    #     def notify_trade(self, trade):
    #         if trade.isclosed:
    #             logger.info(f'TRADE CLOSED: PnL {trade.pnl:.2f}, PnLComm {trade.pnlcomm:.2f}')
    #     def notify_order(self, order):
    #         if order.status in [order.Submitted, order.Accepted]:
    #             return
    #         if order.status in [order.Completed]:
    #             if order.isbuy():
    #                 logger.info(f'BUY EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}')
    #             elif order.issell():
    #                 logger.info(f'SELL EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}')
    #         elif order.status in [order.Canceled, order.Margin, order.Rejected]:
    #             logger.warning(f'Order Canceled/Margin/Rejected: {order.status}')


    # 테스트 기간 설정 (데이터가 있는 기간으로 조정)
    # 현재 DB에 2025-01-29 ~ 2025-05-29 데이터가 81개 있다고 로그에 나왔으므로
    # 이 기간으로 조정합니다.
    end_date = date(2025, 5, 29) # 실제 데이터가 존재하는 마지막 날짜로 설정
    start_date = date(2025, 1, 29) # 실제 데이터가 존재하는 시작 날짜로 설정

    # Backtester 인스턴스 생성
    backtester = Backtester(start_date=start_date, end_date=end_date)

    # 데이터 추가
    test_stock_code = 'A005930' # 삼성전자
    backtester.add_data(stock_code=test_stock_code, timeframe='daily')

    # --- 전략 추가 (TestStrategy 대신 SimpleMAStrategy 사용) ---
    backtester.add_strategy(SimpleMAStrategy, sma_fast_period=5, sma_slow_period=20)
    # --- 전략 추가 끝 ---

    # 백테스팅 실행
    logger.info(f"초기 자산: {backtester.cash:,.0f}원")
    strategies = backtester.run()

    if strategies:
        final_portfolio_value = backtester.cerebro.broker.getvalue()
        logger.info(f"최종 포트폴리오 가치: {final_portfolio_value:,.0f}원")
        total_pnl = final_portfolio_value - backtester.cash
        logger.info(f"총 손익: {total_pnl:,.0f}원")

        # 백테스팅 결과 출력 (백테스팅 결과 시각화는 다음 단계에서)
        # 예를 들어, 최종 포트폴리오 가치 변화를 그래프로 그리려면 cerebro.plot()을 사용할 수 있습니다.
        # cerebro.plot()은 matplotlib을 사용하므로, GUI 환경에서 실행해야 합니다.
        # 현재는 터미널 실행이므로 결과 값만 확인합니다.
        # self.cerebro.plot() # 주석 처리, 나중에 GUI 환경에서 사용할 때 활성화
    else:
        logger.error("백테스팅이 정상적으로 실행되지 않았습니다.")