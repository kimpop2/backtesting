# backtesting/api_client/creon_api.py

import win32com.client
import ctypes
import time
import logging
import pandas as pd
from datetime import datetime, date, timedelta
import re
import os
import sys

# sys.path에 프로젝트 루트 추가 (config.settings 임포트를 위함)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# settings.py에서 필요한 설정 임포트 (현재는 없음)
# from config.settings import API_CONNECT_TIMEOUT, API_REQUEST_INTERVAL

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class CreonAPIClient:
    def __init__(self):
        self.connected = False
        self.cp_code_mgr = None
        self.cp_cybos = None
        self.stock_name_dic = {} # 종목명 -> 코드
        self.stock_code_dic = {} # 코드 -> 종목명
        self._connect_creon()
        if self.connected:
            self.cp_code_mgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")
            logger.info("CpCodeMgr COM object initialized.")
            self._make_stock_dic()

    def _connect_creon(self):
        """Creon Plus에 연결하고 COM 객체를 초기화합니다."""
        if ctypes.windll.shell32.IsUserAnAdmin():
            logger.info("관리자 권한으로 실행 중입니다.")
        else:
            logger.warning("관리자 권한으로 실행되고 있지 않습니다. 일부 Creon 기능이 제한될 수 있습니다.")

        self.cp_cybos = win32com.client.Dispatch("CpUtil.CpCybos")
        if self.cp_cybos.IsConnect:
            self.connected = True
            logger.info("Creon Plus HTS가 이미 연결되어 있습니다.")
        else:
            logger.info("Creon Plus HTS에 연결을 시도합니다...")
            # self.cp_cybos.PlusConnect() # 이 함수는 로그인 팝업을 띄우므로 자동화 시에는 비활성
            # 보통은 HTS가 먼저 실행되고 로그인되어 있어야 함

            max_retries = 10
            for i in range(max_retries):
                if self.cp_cybos.IsConnect:
                    self.connected = True
                    logger.info("Creon Plus HTS에 성공적으로 연결되었습니다.")
                    break
                else:
                    logger.warning(f"Creon Plus HTS 연결 대기 중... ({i+1}/{max_retries})")
                    time.sleep(2)
            if not self.connected:
                logger.error("Creon Plus HTS 연결에 실패했습니다. HTS가 실행 중이고 로그인되어 있는지 확인하세요.")
                raise ConnectionError("Creon Plus HTS 연결 실패.")

    def _check_creon_status(self):
        """Creon API 사용 가능한지 상태를 확인하고 요청 제한에 걸리지 않도록 대기합니다."""
        if not self.connected:
            logger.error("Creon Plus HTS에 연결되어 있지 않습니다.")
            return False

        # 통신 가능 여부 체크
        if self.cp_cybos.IsCommConnect == 0:
            logger.error("통신 연결이 되지 않았습니다. HTS 로그인 상태를 확인하세요.")
            return False

        # 연속 조회 제한 체크 (초당/분당 제한 횟수 확인)
        # 0: 주문 관련, 1: 시세 관련, 2: 실시간 시세
        remain_time = self.cp_cybos.GetLimitRequestRemainTime()
        remain_count_per_sec = self.cp_cybos.GetLimitSecRemainQty(1) # 시세 연속 조회 제한 (초당)
        remain_count_per_min = self.cp_cybos.GetLimitMinRemainQty(1) # 시세 연속 조회 제한 (분당)

        # 초당 제한 체크
        if remain_count_per_sec <= 0:
            logger.info(f"Creon API 초당 요청 제한에 걸렸습니다. 남은 시간: {remain_time/1000:.1f}초. 대기합니다.")
            time.sleep(remain_time / 1000 + 0.5) # 여유를 두고 대기
            if self.cp_cybos.GetLimitSecRemainQty(1) <= 0: # 다시 확인
                logger.warning("API 초당 제한이 풀리지 않았습니다. 추가 대기합니다.")
                time.sleep(1) # 추가 대기

        # 분당 제한 체크
        if remain_count_per_min <= 0:
            logger.info(f"Creon API 분당 요청 제한에 걸렸습니다. 남은 시간: {self.cp_cybos.GetLimitMinRemainTime(1)/1000:.1f}초. 대기합니다.")
            time.sleep(self.cp_cybos.GetLimitMinRemainTime(1) / 1000 + 0.5) # 여유를 두고 대기
            if self.cp_cybos.GetLimitMinRemainQty(1) <= 0: # 다시 확인
                logger.warning("API 분당 제한이 풀리지 않았습니다. 추가 대기합니다.")
                time.sleep(5) # 추가 대기

        return True

    def _is_spac(self, code_name):
        """종목명에 숫자+'호' 패턴이 있으면 스펙주로 판단합니다."""
        return re.search(r'\d+호', code_name) is not None

    def _is_preferred_stock(self, code_name):
        """더 포괄적인 우선주 판단"""
        return re.search(r'([0-9]+우|[가-힣]우[A-Z]?)$', code_name) is not None and len(code_name) >= 3

    def _is_reits(self, code_name):
        """종목명에 '리츠'가 포함되면 리츠로 판단합니다."""
        return "리츠" in code_name

    def _make_stock_dic(self):
        """주식 종목 정보를 딕셔너리로 저장합니다. 스펙주, 우선주, 리츠, ETF, ETN, ELW, 뮤추얼펀드 제외."""
        logger.info("종목 코드/명 딕셔너리 생성 시작")
        if not self.cp_code_mgr:
            logger.error("cp_code_mgr이 초기화되지 않아 종목 딕셔너리를 생성할 수 없습니다.")
            return

        try:
            kospi_codes = self.cp_code_mgr.GetStockListByMarket(1) # KOSPI
            kosdaq_codes = self.cp_code_mgr.GetStockListByMarket(2) # KOSDAQ
            all_codes = kospi_codes + kosdaq_codes

            processed_count = 0
            for code in all_codes:
                code_name = self.cp_code_mgr.CodeToName(code)
                if not code_name: # 종목명 없으면 스킵
                    continue

                # 섹션 종류 필터링 (0:보통주, 1:우선주, 2:뮤추얼펀드, 3:ETF, 4:ETN, 5:ELW, 6:워런트, 7:스펙, 8:리츠)
                # 제외할 섹션: 1, 2, 3, 4, 5, 6, 7, 8
                section_kind = self.cp_code_mgr.GetStockSectionKind(code)
                if section_kind in [1, 2, 3, 4, 5, 6, 7, 8]:
                    continue

                # 추가 이름 기반 필터링
                if (self._is_spac(code_name) or
                    self._is_preferred_stock(code_name) or
                    self._is_reits(code_name)):
                    continue

                # 관리/투자경고/위험 등 종목 필터링
                if self.cp_code_mgr.GetStockControlKind(code) != 0: # 0: 정상
                    continue
                if self.cp_code_mgr.GetStockSupervisionKind(code) != 0: # 0: 정상
                    continue
                if self.cp_code_mgr.GetStockStatusKind(code) in [2, 3]: # 2: 거래정지, 3: 거래중단
                    continue

                self.stock_name_dic[code_name] = code
                self.stock_code_dic[code] = code_name
                processed_count += 1

            logger.info(f"종목 코드/명 딕셔너리 생성 완료. 총 {processed_count}개 종목 저장.")

        except Exception as e:
            logger.error(f"_make_stock_dic 중 오류 발생: {e}", exc_info=True)

    def get_stock_name(self, find_code):
        """종목코드로 종목명을 반환 합니다."""
        return self.stock_code_dic.get(find_code, None)

    def get_stock_code(self, find_name):
        """종목명으로 종목목코드를 반환 합니다."""
        return self.stock_name_dic.get(find_name, None)

    def get_filtered_stock_list(self):
        """필터링된 모든 종목 코드를 리스트로 반환합니다."""
        return list(self.stock_code_dic.keys())

    def _get_price_data(self, stock_code, period, from_date_str, to_date_str, interval=1):
        """
        Creon API에서 주식 차트 데이터를 가져오는 내부 범용 메서드 (연속 조회 지원).
        :param stock_code: 종목 코드 (예: 'A005930')
        :param period: 'D': 일봉, 'W': 주봉, 'M': 월봉, 'm': 분봉
        :param from_date_str: 시작일 (YYYYMMDD 형식 문자열)
        :param to_date_str: 종료일 (YYYYMMDD 형식 문자열)
        :param interval: 분봉일 경우 주기 (기본 1분)
        :return: Pandas DataFrame
        """
        if not self._check_creon_status():
            return pd.DataFrame()

        objChart = win32com.client.Dispatch('CpSysDib.StockChart')

        # 입력 값 설정 (기간으로 요청)
        objChart.SetInputValue(0, stock_code)
        objChart.SetInputValue(1, ord('1'))      # 요청구분 1:기간, 2:개수
        objChart.SetInputValue(2, int(to_date_str))    # To 날짜
        objChart.SetInputValue(3, int(from_date_str))  # From 날짜
        objChart.SetInputValue(6, ord(period))   # 주기
        objChart.SetInputValue(9, ord('1'))      # 수정주가 사용: 1(사용)

        # 요청 항목 설정 (DB 스키마에 맞춰 필드 인덱스 지정)
        if period == 'm': # 분봉
            # 0: 날짜, 1: 시간, 2: 시가, 3: 고가, 4: 저가, 5: 종가, 8: 거래량
            # GetDataValue의 인덱스는 SetInputValue(5, ...)에 설정된 필드들의 순서에 따름
            requested_fields = [0, 1, 2, 3, 4, 5, 8]
            objChart.SetInputValue(7, interval)  # 분틱차트 주기 (분봉일 때만 필요)
        else: # 일봉, 주봉, 월봉
            # 0: 날짜, 2: 시가, 3: 고가, 4: 저가, 5: 종가, 8: 거래량, 9: 거래대금
            # Creon API 문서에 따르면 GetDataValue(0,i) ~ GetDataValue(N,i)는
            # SetInputValue(5, tuple)에 정의된 순서대로 값을 반환합니다.
            requested_fields = [0, 2, 3, 4, 5, 8, 9] # 날짜, 시가, 고가, 저가, 종가, 거래량, 거래대금

        objChart.SetInputValue(5, requested_fields) # 요청할 데이터 필드

        data_list = []

        while True:
            objChart.BlockRequest()
            # 연속 조회 요청 시 API 제한을 준수하기 위한 대기
            # GetLimitRequestRemainTime() 대신 GetLimitSecRemainQty(1) 등으로 대체 확인
            self._check_creon_status() # 요청 후에도 상태 다시 체크 및 대기

            rq_status = objChart.GetDibStatus()
            rq_msg = objChart.GetDibMsg1()

            if rq_status != 0:
                logger.error(f"CpStockChart: 데이터 요청 실패. 통신상태: {rq_status}, 메시지: {rq_msg}")
                # 오류 코드 5는 '해당 기간의 데이터 없음'을 의미할 수 있음
                if rq_status == 5:
                    logger.warning(f"지정된 기간({from_date_str}~{to_date_str}) 동안 {stock_code}에 대한 데이터가 없습니다.")
                return pd.DataFrame() # 빈 DataFrame 반환

            received_len = objChart.GetHeaderValue(3) # 현재 BlockRequest로 수신된 데이터 개수
            if received_len == 0:
                break # 더 이상 받을 데이터가 없으면 루프 종료

            for i in range(received_len):
                if period == 'm':
                    date_val = objChart.GetDataValue(0, i) # 날짜 (YYYYMMDD)
                    time_val = objChart.GetDataValue(1, i) # 시간 (HHMM)
                    dt_str = f"{date_val}{time_val:04d}" # 시간을 4자리로 채움 (예: 930 -> 0930)
                    dt_obj = datetime.strptime(dt_str, '%Y%m%d%H%M')

                    data_list.append({
                        'stock_code': stock_code,
                        'datetime': dt_obj, # 분봉은 datetime 컬럼
                        'open_price': objChart.GetDataValue(2, i),
                        'high_price': objChart.GetDataValue(3, i),
                        'low_price': objChart.GetDataValue(4, i),
                        'close_price': objChart.GetDataValue(5, i),
                        'volume': objChart.GetDataValue(6, i) # 요청 필드 중 8번째인 거래량
                    })
                else: # 일봉, 주봉, 월봉
                    date_val = objChart.GetDataValue(0, i) # 날짜 (YYYYMMDD)
                    data_list.append({
                        'stock_code': stock_code,
                        'date': datetime.strptime(str(date_val), '%Y%m%d').date(), # 일봉은 date 컬럼
                        'open_price': objChart.GetDataValue(1, i), # 요청 필드 중 2번째인 시가
                        'high_price': objChart.GetDataValue(2, i), # 요청 필드 중 3번째인 고가
                        'low_price': objChart.GetDataValue(3, i), # 요청 필드 중 4번째인 저가
                        'close_price': objChart.GetDataValue(4, i), # 요청 필드 중 5번째인 종가
                        'volume': objChart.GetDataValue(5, i), # 요청 필드 중 8번째인 거래량
                        'change_rate': None, # StockDataManager에서 계산 예정
                        'trading_value': objChart.GetDataValue(6, i) # 요청 필드 중 9번째인 거래대금
                    })

            if not objChart.Continue: # 연속해서 받을 데이터가 없으면 종료
                break

        df = pd.DataFrame(data_list)
        # Creon API는 최신 데이터부터 과거 데이터 순으로 반환하므로, 오름차순으로 정렬
        if not df.empty:
            if period == 'm':
                df = df.sort_values(by='datetime', ascending=True).reset_index(drop=True)
            else:
                df = df.sort_values(by='date', ascending=True).reset_index(drop=True)

        logger.info(f"{stock_code} {period} 데이터 {len(df)}개 조회 완료. ({from_date_str} ~ {to_date_str})")
        return df

    def get_daily_ohlcv(self, stock_code, start_date_str, end_date_str):
        """
        특정 종목의 일봉 OHLCV 데이터를 Creon API에서 가져옵니다.
        :param stock_code: 종목 코드 (예: 'A005930')
        :param start_date_str: 시작일 (YYYYMMDD 형식 문자열)
        :param end_date_str: 종료일 (YYYYMMDD 형식 문자열)
        :return: Pandas DataFrame
        """
        return self._get_price_data(stock_code, 'D', start_date_str, end_date_str)

    def get_minute_ohlcv(self, stock_code, start_date_str, end_date_str, interval=1):
        """
        특정 종목의 분봉 OHLCV 데이터를 Creon API에서 가져옵니다.
        :param stock_code: 종목 코드 (예: 'A005930')
        :param start_date_str: 시작일 (YYYYMMDD 형식 문자열)
        :param end_date_str: 종료일 (YYYYMMDD 형식 문자열)
        :param interval: 분봉 주기 (기본 1분)
        :return: Pandas DataFrame
        """
        return self._get_price_data(stock_code, 'm', start_date_str, end_date_str, interval)

    def get_financial_data(self, stock_code, period_type='annual', count=5):
        """
        Creon API를 통해 종목의 재무 데이터를 조회합니다.
        :param stock_code: 종목 코드
        :param period_type: 'annual' (연간) 또는 'quarter' (분기별)
        :param count: 조회할 기간 개수 (최근 N년/분기)
        :return: Pandas DataFrame
        """
        try:
            self._check_creon_status()

            obj = win32com.client.Dispatch("CpSysDib.CpSvr7254") 
            obj.SetInputValue(0, stock_code) # 종목코드
            obj.SetInputValue(1, 0 if period_type == 'annual' else 1) # 0:년도, 1:분기
            obj.SetInputValue(2, count) # 요청할 기간 개수

            obj.BlockRequest()

            rqStatus = obj.GetHeaderValue(0) # 통신상태
            rqRet = obj.GetHeaderValue(1) # 통신결과
            if rqRet != 0:
                logger.error(f"Creon CpSvr7254 요청 실패 (Return Code: {rqRet}) for {stock_code}")
                return pd.DataFrame()

            num_data = obj.GetHeaderValue(2) # 수신 개수

            data_list = []
            for i in range(num_data):
                # CpSvr7254 필드 (Creon API 문서 참조)
                # 0: 결산년월, 1: 결산구분, 2: 매출액, 3: 영업이익, 4: 경상이익, 5: 당기순이익, 6: EPS, 7: BPS, 8: DPS, 9: PER, 10: PBR, 11: ROE, 12: ROA, 13: 부채비율, 14: 유보율 등
                base_date_str = str(obj.GetDataValue(0, i)) # 결산년월 (예: 202312)
                quarter_type = obj.GetDataValue(1, i) # 결산구분 (1:1분기, 2:2분기, 3:3분기, 4:4분기)

                # base_date 형식 변환 (YYYYMM -> YYYY-MM-DD, 해당 분기/년도 말일로 간주)
                year = int(base_date_str[:4])
                month = int(base_date_str[4:])
                if period_type == 'annual':
                    base_date_obj = date(year, 12, 31)
                    quarter = 4 # 연간 데이터는 4분기로 간주
                else: # quarter
                    if month == 3: base_date_obj = date(year, 3, 31)
                    elif month == 6: base_date_obj = date(year, 6, 30)
                    elif month == 9: base_date_obj = date(year, 9, 30)
                    elif month == 12: base_date_obj = date(year, 12, 31)
                    else: base_date_obj = None # 예외 처리
                    quarter = quarter_type

                row = {
                    'stock_code': stock_code,
                    'base_date': base_date_obj,
                    'quarter': quarter,
                    'sales': obj.GetDataValue(2, i), # 매출액
                    'operating_profit': obj.GetDataValue(3, i), # 영업이익
                    'net_profit': obj.GetDataValue(5, i), # 당기순이익
                    'per': obj.GetDataValue(9, i), # PER
                    'pbr': obj.GetDataValue(10, i), # PBR
                    'roe': obj.GetDataValue(11, i), # ROE
                    'debt_ratio': obj.GetDataValue(13, i) # 부채비율
                }
                data_list.append(row)

            df = pd.DataFrame(data_list)
            # base_date를 기준으로 정렬
            df = df.sort_values(by='base_date', ascending=True).reset_index(drop=True)
            logger.info(f"{stock_code} 재무 데이터 {period_type} {len(df)}개 조회 완료.")
            return df

        except pythoncom.com_error as e:
            logger.error(f"COM 오류 발생 (재무 데이터): {e} (Creon HTS가 실행 중인지, API 사용 권한이 있는지 확인하세요.)", exc_info=True)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"재무 데이터 조회 중 오류 발생: {e}", exc_info=True)
            return pd.DataFrame()