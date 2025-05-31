# backtesting/strategies/simple_ma_strategy.py

import backtrader as bt
import logging

logger = logging.getLogger(__name__)

class SimpleMAStrategy(bt.Strategy):
    """
    단순 이동평균(SMA) 교차 전략:
    단기 이동평균(SMA_FAST)이 장기 이동평균(SMA_SLOW)을 상향 돌파하면 매수,
    하향 돌파하면 매도합니다.
    """
    params = (
        ('sma_fast_period', 10), # 단기 이동평균 기간
        ('sma_slow_period', 50), # 장기 이동평균 기간
    )

    def __init__(self):
        # 모든 데이터 피드의 종가(close) 라인을 참조합니다.
        self.dataclose = self.datas[0].close

        # 주문 상태 추적을 위한 변수
        self.order = None

        # 이동평균 지표 계산
        # bt.indicators.SMA(self.dataclose, period=self.p.sma_fast_period)
        # backtrader 튜토리얼에서 보통 self.data.close 를 사용하므로, self.datas[0].close를 넣어줍니다.
        self.sma_fast = bt.indicators.SMA(self.dataclose, period=self.p.sma_fast_period)
        self.sma_slow = bt.indicators.SMA(self.dataclose, period=self.p.sma_slow_period)

        # 이동평균 교차 지표 (Crossover)
        # 단기 SMA가 장기 SMA를 상향/하향 돌파하는지 알려주는 지표
        self.crossover = bt.indicators.CrossOver(self.sma_fast, self.sma_slow)

        logger.info(f"전략 초기화: 단기 SMA({self.p.sma_fast_period}), 장기 SMA({self.p.sma_slow_period})")

    def notify_order(self, order):
        # 주문 상태 변경 알림
        if order.status in [order.Submitted, order.Accepted]:
            # 제출 또는 수락됨 -> 아무것도 하지 않음 (처리 대기 중)
            return

        if order.status in [order.Completed]:
            # 주문 완료
            if order.isbuy():
                logger.info(
                    f'매수 완료: 날짜={self.data.datetime.date(0)}, '
                    f'가격={order.executed.price:.2f}, '
                    f'수량={order.executed.size}, '
                    f'수수료={order.executed.comm:.2f}'
                )
            elif order.issell():
                logger.info(
                    f'매도 완료: 날짜={self.data.datetime.date(0)}, '
                    f'가격={order.executed.price:.2f}, '
                    f'수량={order.executed.size}, '
                    f'수수료={order.executed.comm:.2f}'
                )
            self.bar_executed = len(self) # 주문이 실행된 바(bar)의 인덱스 기록

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            # 주문 취소, 마진 부족, 거부됨
            logger.warning(f'주문 실패: 상태={order.Status[order.status]}, 날짜={self.data.datetime.date(0)}')

        # 주문이 완료되거나 실패하면 self.order를 초기화하여 다음 주문을 낼 수 있도록 합니다.
        self.order = None

    def notify_trade(self, trade):
        # 거래(Trade) 상태 변경 알림 (주문이 체결되어 포지션이 생성/종료될 때)
        if not trade.isclosed:
            return # 아직 닫히지 않은 거래는 무시

        logger.info(f'거래 종료: 총 손익={trade.pnl:.2f}, 수수료 포함 손익={trade.pnlcomm:.2f}')

    def next(self):
        # 다음 데이터 바(bar)가 들어올 때마다 호출됩니다.
        # 이 메서드에 실제 매매 로직을 구현합니다.

        # 현재 진행 중인 주문이 있다면, 새 주문을 내지 않고 기다립니다.
        if self.order:
            return

        # 포지션이 없는 경우
        if not self.position:
            # 단기 이동평균이 장기 이동평균을 상향 돌파 (매수 신호)
            # crossover > 0 : 상향 돌파 (단기선이 장기선을 위로 통과)
            if self.crossover[0] > 0: # crossover[0]는 현재 바(bar)에서의 crossover 값
                logger.info(f'매수 신호 발생: 날짜={self.data.datetime.date(0)}, 단기SMA={self.sma_fast[0]:.2f}, 장기SMA={self.sma_slow[0]:.2f}')
                # 시장가 매수 주문 (전체 현금의 90% 사용)
                # size는 매수할 수량입니다. backtrader가 알아서 계산해줍니다.
                # backtrader.py에서 setcash(100_000_000)으로 설정했으므로
                # buy(size=XX)를 호출할 때 브로커의 현재 현금과 종목 가격을 고려합니다.
                # size를 지정하지 않으면 (기본 1) 오류가 날 수 있으므로, strategy에서 set_param을 통해 size를 지정하거나
                # 또는 backtester에서 set_order_size를 통해 일괄 지정하는 것이 좋습니다.
                # 여기서는 일단 10주 매수 예시로 진행합니다. (실제 자산에 따라 조절 필요)
                self.order = self.buy(size=10) # 10주 매수 예시

        # 포지션이 있는 경우
        else:
            # 단기 이동평균이 장기 이동평균을 하향 돌파 (매도 신호)
            # crossover < 0 : 하향 돌파 (단기선이 장기선을 아래로 통과)
            if self.crossover[0] < 0:
                logger.info(f'매도 신호 발생: 날짜={self.data.datetime.date(0)}, 단기SMA={self.sma_fast[0]:.2f}, 장기SMA={self.sma_slow[0]:.2f}')
                # 시장가 매도 주문 (현재 보유한 모든 포지션 매도)
                self.order = self.sell(size=self.position.size) # 모든 포지션 매도