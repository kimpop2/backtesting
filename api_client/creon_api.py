# backtesting/api_client/creon_api.py

import win32com.client
import ctypes
import time
import logging
import pandas as pd
from datetime import datetime, timedelta, date # date 임포트 추가
import re
import os
import sys

# sys.path에 프로젝트 루트 추가 (모듈 임포트를 위함)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# 로거 설정 (기존 설정 유지)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CreonAPIClient:
    def __init__(self):
        self.connected = False
        self.cp_code_mgr = None
        self.cp_cybos = None
        self.stock_name_dic = {}
        self.stock_code_dic = {}
        self._connect_creon()
        if self.connected:
            self.cp_code_mgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")
            logger.info("CpCodeMgr COM object initialized.")
            self._make_stock_dic()

    def _connect_creon(self):
        """Creon Plus에 연결하고 COM 객체를 초기화합니다."""
        if ctypes.windll.shell32.IsUserAnAdmin():
            logger.info("Running with administrator privileges.")
        else:
            logger.warning("Not running with administrator privileges. Some Creon functions might be restricted.")

        self.cp_cybos = win32com.client.Dispatch("CpUtil.CpCybos")
        if self.cp_cybos.IsConnect:
            self.connected = True
            logger.info("Creon Plus is already connected.")
        else:
            logger.info("Attempting to connect to Creon Plus...")
            # self.cp_cybos.PlusConnect()
            max_retries = 10
            for i in range(max_retries):
                if self.cp_cybos.IsConnect:
                    self.connected = True
                    logger.info("Creon Plus connected successfully.")
                    break
                else:
                    logger.warning(f"Waiting for Creon Plus connection... ({i+1}/{max_retries})")
                    time.sleep(2)
            if not self.connected:
                logger.error("Failed to connect to Creon Plus. Please ensure HTS is running and logged in.")
                raise ConnectionError("Creon Plus connection failed.")

    def _check_creon_status(self):
        """Creon API 사용 가능한지 상태를 확인합니다."""
        if not self.connected:
            logger.error("Creon Plus is not connected.")
            return False

        # 요청 제한 개수 확인 (기존 코드 유지)
        # remain_count = self.cp_cybos.GetLimitRequestRemainTime() # 인자 제거
        # if remain_count <= 0:
        #      logger.warning(f"Creon API request limit reached. Waiting for 1 second.")
        #      time.sleep(1)
        #      remain_count = self.cp_cybos.GetLimitRequestRemainTime() # 인자 제거
        #      if remain_count <= 0:
        #          logger.error("Creon API request limit still active after waiting. Cannot proceed.")
        #          return False
        return True

    def _is_spac(self, code_name):
        """종목명에 숫자+'호' 패턴이 있으면 스펙주로 판단합니다."""
        return re.search(r'\d+호', code_name) is not None

    def _is_preferred_stock(self, code):
        """우선주 판단, 코드 뒷자리가 0이 아님"""
        return code[-1] != '0'

    def _is_reits(self, code_name):
        """종목명에 '리츠'가 포함되면 리츠로 판단합니다."""
        return "리츠" in code_name

    def _make_stock_dic(self):
        """주식 종목 정보를 딕셔너리로 저장합니다. 스펙주, 우선주, 리츠 제외."""
        logger.info("종목 코드/명 딕셔너리 생성 시작")
        if not self.cp_code_mgr:
            logger.error("cp_code_mgr is not initialized. Cannot make stock dictionary.")
            return

        try:
            kospi_codes = self.cp_code_mgr.GetStockListByMarket(1)
            kosdaq_codes = self.cp_code_mgr.GetStockListByMarket(2)
            all_codes = kospi_codes + kosdaq_codes
            
            processed_count = 0
            for code in all_codes:
                code_name = self.cp_code_mgr.CodeToName(code)
                if not code_name: # 종목명이 없으면 유효하지 않은 종목으로 간주
                    continue

                # 1. 섹션 종류 필터링: 보통주(0)만 포함
                # Creon API GetStockSectionKind: 0:보통주, 1:우선주, 2:뮤추얼펀드, 3:ETF, 4:ETN, 5:ELW, 6:워런트, 7:스펙, 8:리츠
                section_kind = self.cp_code_mgr.GetStockSectionKind(code)
                if section_kind != 1: # 보통주(1)가 아니면 다음 종목으로 건너뛰기
                    continue

                # 2. 이름 기반 필터링 (섹션 종류가 1이어도 이름으로 추가 확인)
                if (self._is_spac(code_name) or
                    self._is_preferred_stock(code) or
                    self._is_reits(code_name)):
                    continue

                # 3. 관리/투자경고/거래정지 등 상태 필터링
                # GetStockControlKind: 0:정상, 1:관리, 2:투자경고, 3:투자위험, 4:투자주의 등
                if self.cp_code_mgr.GetStockControlKind(code) != 0: 
                    continue
                # GetStockSupervisionKind: 0:정상, 1:투자유의
                if self.cp_code_mgr.GetStockSupervisionKind(code) != 0: 
                    continue
                # GetStockStatusKind: 0:정상, 2:거래정지, 3:거래중단
                if self.cp_code_mgr.GetStockStatusKind(code) in [2, 3]: 
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
        Creon API에서 주식 차트 데이터를 가져오는 내부 범용 메서드.
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
        
        # 입력 값 설정
        objChart.SetInputValue(0, stock_code)
        objChart.SetInputValue(1, ord('1'))    # 요청구분 1:기간 2: 개수 (우리는 기간으로 요청)
        objChart.SetInputValue(2, int(to_date_str))   # 2: To 날짜 (long)
        objChart.SetInputValue(3, int(from_date_str)) # 3: From 날짜 (long)
        objChart.SetInputValue(6, ord(period)) # 주기
        objChart.SetInputValue(9, ord('1'))    # 수정주가 사용

        # 요청 항목 설정 (주기에 따라 달라짐)
        if period == 'm':
            objChart.SetInputValue(7, interval)  # 분틱차트 주기 (1분)
            # 요청 항목: 날짜(0), 시간(1), 시가(2), 고가(3), 저가(4), 종가(5), 거래량(8)
            requested_fields = [0, 1, 2, 3, 4, 5, 8] # Creon API의 필드 인덱스
        else: # 일봉, 주봉, 월봉
            # 요청 항목: 날짜(0), 시가(2), 고가(3), 저가(4), 종가(5), 거래량(8), 거래대금(9)
            requested_fields = [0, 2, 3, 4, 5, 8, 9] # 거래대금(9) 필드 추가
            
        objChart.SetInputValue(5, requested_fields) # 요청할 데이터

        data_list = []
        
        while True:
            objChart.BlockRequest()
            time.sleep(0.2) # 과도한 요청 방지 및 제한 시간 준수

            rq_status = objChart.GetDibStatus()
            rq_msg = objChart.GetDibMsg1()

            if rq_status != 0:
                logger.error(f"CpStockChart: 데이터 요청 실패. 통신상태: {rq_status}, 메시지: {rq_msg}")
                # 오류 코드 5는 '해당 기간의 데이터 없음'을 의미할 수 있음
                if rq_status == 5:
                    logger.warning(f"No data for {stock_code} in specified period ({from_date_str}~{to_date_str}).")
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
                        'volume': objChart.GetDataValue(6, i) # 필드 8(거래량)의 실제 인덱스는 6
                        # 'trading_value'는 분봉에서 요청하지 않음
                    })
                else: # 일봉, 주봉, 월봉
                    date_val = objChart.GetDataValue(0, i)
                    data_list.append({
                        'stock_code': stock_code,
                        'date': datetime.strptime(str(date_val), '%Y%m%d').date(), # 일봉은 date 컬럼
                        'open_price': objChart.GetDataValue(1, i), # 요청필드 인덱스 2
                        'high_price': objChart.GetDataValue(2, i), # 요청필드 인덱스 3
                        'low_price': objChart.GetDataValue(3, i), # 요청필드 인덱스 4
                        'close_price': objChart.GetDataValue(4, i), # 요청필드 인덱스 5
                        'volume': objChart.GetDataValue(5, i), # 요청필드 인덱스 8
                        'change_rate': None, # 추후 계산
                        'trading_value': objChart.GetDataValue(6, i) # 요청필드 인덱스 9 (거래대금)
                    })
            
            if not objChart.Continue:
                break # 더 이상 연속 조회할 데이터가 없으면 종료

        df = pd.DataFrame(data_list)
        # Creon API는 최신 데이터부터 과거 데이터 순으로 반환하므로, 오름차순으로 정렬
        if not df.empty:
            if period == 'm':
                df = df.sort_values(by='datetime').reset_index(drop=True)
            else:
                df = df.sort_values(by='date').reset_index(drop=True)
        return df

    def get_daily_ohlcv(self, stock_code, start_date_str, end_date_str):
        """
        특정 종목의 일봉 OHLCV 데이터를 Creon API에서 가져옵니다.
        :param stock_code: 종목 코드 (예: 'A005930')
        :param start_date_str: 시작일 (YYYYMMDD 형식 문자열)
        :param end_date_str: 종료일 (YYYYMMDD 형식 문자열)
        :return: Pandas DataFrame
        """
        logger.info(f"Fetching daily data for {stock_code} from {start_date_str} to {end_date_str}")
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
        logger.info(f"Fetching {interval}-minute data for {stock_code} from {start_date_str} to {end_date_str}")
        return self._get_price_data(stock_code, 'm', start_date_str, end_date_str, interval)

    def get_latest_financial_data(self, stock_code): # 메서드명 변경 (혼동 방지)
        """
        CpSysDib.MarketEye를 사용하여 종목의 최신 재무 데이터를 조회합니다.
        이는 '스냅샷' 데이터로, 가장 최근의 결산/분기 데이터를 반환합니다.
        :param stock_code: 종목 코드
        :return: Pandas DataFrame (단일 행), 데이터가 없으면 빈 DataFrame 반환
        """
        try:
            if not self._check_creon_status():
                return pd.DataFrame()

            objMarketEye = win32com.client.Dispatch("CpSysDib.MarketEye")
            
            # 요청할 필드 설정 (stock_info 테이블에 추가된 컬럼에 맞춰 확장)
            req_fields = [
                17,   # 종목명
                67,   # PER
                70,   # EPS
                75,   # 부채비율
                # 76,   # 유보율 - stock_info에 없으므로 제외
                77,   # ROE (자기자본순이익률)
                86,   # 매출액 (단위: 백만)
                91,   # 영업이익 (단위: 원)
                88,   # 당기순이익 (단위: 원)
                # 89,   # BPS (주당순자산) - stock_info에 없으므로 제외
                95,   # 결산년월 (연간 재무데이터 기준)
                111   # 최근분기년월 (분기 재무데이터 기준)
            ]

            objMarketEye.SetInputValue(0, req_fields)   # 요청할 필드 배열
            objMarketEye.SetInputValue(1, stock_code)   # 종목 코드 (단일 종목 요청)

            objMarketEye.BlockRequest()

            # 요청 상태 확인
            rq_status = objMarketEye.GetDibStatus()
            rq_msg = objMarketEye.GetDibMsg1()

            if rq_status != 0:
                logger.error(f"MarketEye 재무 데이터 요청 실패. 통신상태: {rq_status}, 메시지: {rq_msg} for {stock_code}")
                if rq_status == 5: # 오류 코드 5는 '해당 데이터 없음'을 의미할 수 있음
                    logger.warning(f"{stock_code}에 대한 MarketEye 재무 데이터가 없습니다.")
                return pd.DataFrame()

            num_stocks = objMarketEye.GetHeaderValue(2) # 종목 개수 (단일 종목이므로 1)

            if num_stocks == 0:
                logger.warning(f"{stock_code}에 대한 MarketEye 재무 데이터가 없습니다.")
                return pd.DataFrame()

            # 데이터 추출
            data = {
                'stock_code': stock_code,
                'stock_name': objMarketEye.GetDataValue(req_fields.index(17), 0),
                'per': objMarketEye.GetDataValue(req_fields.index(67), 0),
                'eps': objMarketEye.GetDataValue(req_fields.index(70), 0),
                'debt_ratio': objMarketEye.GetDataValue(req_fields.index(75), 0),
                'roe': objMarketEye.GetDataValue(req_fields.index(77), 0),
                'sales': objMarketEye.GetDataValue(req_fields.index(86), 0), # 백만 원 단위
                'operating_profit': objMarketEye.GetDataValue(req_fields.index(91), 0), # 원 단위
                'net_profit': objMarketEye.GetDataValue(req_fields.index(88), 0), # 원 단위
                'annual_base_date_str': str(objMarketEye.GetDataValue(req_fields.index(95), 0)), # YYYYMM
                'quarter_base_date_str': str(objMarketEye.GetDataValue(req_fields.index(111), 0)) # YYYYMM
            }

            df = pd.DataFrame([data])

            # 최신 재무 데이터의 기준 일자 결정 (연간 또는 분기 중 더 최근 데이터)
            # MarketEye는 '최신' 재무 데이터의 기준년월을 제공하므로,
            # 'recent_financial_date' 컬럼에는 해당 연월의 마지막 날짜를 넣어주는 것이 일반적입니다.
            def convert_yyyymm_to_date_end_of_month(yyyymm_str):
                if yyyymm_str and yyyymm_str != '0':
                    try:
                        year = int(yyyymm_str[:4])
                        month = int(yyyymm_str[4:])
                        if 1 <= month <= 12:
                            # 해당 월의 마지막 날짜 계산
                            return date(year, month, 1) + timedelta(days=31) - timedelta(days=(date(year, month, 1) + timedelta(days=31)).day)
                    except ValueError:
                        return None
                return None
            
            df['annual_base_date'] = df['annual_base_date_str'].apply(convert_yyyymm_to_date_end_of_month)
            df['quarter_base_date'] = df['quarter_base_date_str'].apply(convert_yyyymm_to_date_end_of_month)

            # 더 최신인 날짜를 recent_financial_date로 선택 (둘 다 None이면 None)
            df['recent_financial_date'] = df.apply(
                lambda row: max(filter(None, [row['annual_base_date'], row['quarter_base_date']]), default=None),
                axis=1
            )
            # pbr은 EPS와 현재가로 직접 계산하거나, MarketEye의 PER/BPS 조합으로 계산
            # MarketEye 필드 67: PER, 89: BPS, 96: 현재가 -> pbr = 현재가 / BPS
            # 또는 PBR 필드 68을 직접 요청할 수 있지만, 여기서는 기존 EPS/PER/BPS 조합을 유지
            # PBR 68 필드를 MarketEye 요청에 추가하여 직접 가져오는 것이 더 정확하고 간단.
            # 하지만 최소한의 변경을 위해 현재는 pbr 필드를 명시적으로 요청하지 않음.
            # stock_info 스키마에 PBR이 있으므로, PBR도 MarketEye 필드 68을 추가하여 가져오는 것이 좋습니다.
            # 지금은 pbr 값을 0으로 채워 넣고, 필요시 `get_latest_financial_data`를 수정하여 가져오도록 합니다.
            # 현재 코드에서는 pbr을 요청하지 않으므로, 데이터프레임에 추가하지 않습니다.
            # DB Manager에서 None 값을 허용하므로, 해당 값이 없으면 자동으로 NULL로 들어갑니다.
            
            logger.info(f"{stock_code} MarketEye 최신 재무 데이터 조회 완료.")
            return df[['stock_code', 'stock_name', 'per', 'eps', 'debt_ratio', 'roe',
                       'sales', 'operating_profit', 'net_profit', 'recent_financial_date']]

        except Exception as e:
            logger.error(f"MarketEye 재무 데이터 조회 중 오류 발생: {e}", exc_info=True)
            return pd.DataFrame()