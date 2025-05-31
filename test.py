import win32com.client
import pandas as pd
import time
import datetime

class CreonFinancialData:
    def __init__(self):
        """CREON API 초기화"""
        self.objCpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
        self.objCpCodeMgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")
        self.objCpTrade = win32com.client.Dispatch("CpTrade.CpTdUtil")
        
        # 연결 확인
        self.check_connection()
    
    def format_stock_code(self, stock_code):
        """종목코드 포맷팅 - 앞에 A 붙이기"""
        if not stock_code.startswith('A'):
            return 'A' + stock_code
        return stock_code
    
    def check_connection(self):
        """CREON 연결 상태 확인"""
        if self.objCpCybos.IsConnect == 0:
            print("CREON Plus가 실행되지 않았습니다.")
            return False
        print("CREON Plus 연결 완료")
        return True
    
    def get_stock_code_list(self, market_type=1):
        """
        주식 코드 리스트 조회
        market_type: 1=코스피, 2=코스닥
        """
        code_list = []
        if market_type == 1:
            code_count = self.objCpCodeMgr.GetCount(1)  # 코스피
            for i in range(code_count):
                code = self.objCpCodeMgr.GetData(1, i, 0)  # 종목코드
                name = self.objCpCodeMgr.GetData(1, i, 1)  # 종목명
                code_list.append({'code': code, 'name': name})
        elif market_type == 2:
            code_count = self.objCpCodeMgr.GetCount(2)  # 코스닥
            for i in range(code_count):
                code = self.objCpCodeMgr.GetData(2, i, 0)
                name = self.objCpCodeMgr.GetData(2, i, 1)
                code_list.append({'code': code, 'name': name})
        
        return code_list
    
    def get_financial_data(self, stock_code, period_type='annual', count=5):
        """
        Creon API를 통해 종목의 재무 데이터를 조회합니다.
        :param stock_code: 종목 코드
        :param period_type: 'annual' (연간) 또는 'quarter' (분기별)
        :param count: 조회할 기간 개수 (최근 N년/분기)
        :return: Pandas DataFrame
        """
    def get_financial_data(self, stock_code, period_type='annual', count=5):
        """
        Creon API를 통해 종목의 재무 데이터를 조회합니다.
        :param stock_code: 종목 코드
        :param period_type: 'annual' (연간) 또는 'quarter' (분기별)
        :param count: 조회할 기간 개수 (최근 N년/분기)
        :return: Pandas DataFrame
        """
        # 종목코드 포맷팅
        formatted_code = self.format_stock_code(stock_code)
        
        try:
            # CpSysDib.StockMst - 현재 주가 및 기본 정보
            objRq = win32com.client.Dispatch("CpSysDib.StockMst")
            objRq.SetInputValue(0, formatted_code)
            objRq.BlockRequest()
            
            # 현재 주가 정보
            current_price = objRq.GetHeaderValue(11)  # 현재가
            per = objRq.GetHeaderValue(13)  # PER
            pbr = objRq.GetHeaderValue(14)  # PBR
            
            # 재무제표 데이터 조회 (CpSysDib.CpSvr8561T)
            objRq2 = win32com.client.Dispatch("CpSysDib.CpSvr8561T")
            objRq2.SetInputValue(0, formatted_code)
            
            # 기간 타입 설정
            if period_type == 'annual':
                objRq2.SetInputValue(1, ord('A'))  # 연간
            elif period_type == 'quarter':
                objRq2.SetInputValue(1, ord('Q'))  # 분기별
            else:
                objRq2.SetInputValue(1, ord('A'))  # 기본값: 연간
                
            objRq2.SetInputValue(2, count)     # 조회 개수
            objRq2.BlockRequest()
            
            # 데이터 추출
            financial_data = []
            
            for i in range(objRq2.GetHeaderValue(0)):  # 데이터 개수
                period = objRq2.GetDataValue(0, i)     # 년도/분기
                revenue = objRq2.GetDataValue(1, i)    # 매출액
                op_profit = objRq2.GetDataValue(2, i)  # 영업이익
                net_profit = objRq2.GetDataValue(3, i) # 당기순이익
                
                financial_data.append({
                    'period': period,
                    'revenue': revenue,
                    'operating_profit': op_profit,
                    'net_profit': net_profit
                })
            
            # ROE 및 부채비율 조회 (CpSysDib.CpSvr8563T)
            objRq3 = win32com.client.Dispatch("CpSysDib.CpSvr8563T")
            objRq3.SetInputValue(0, formatted_code)
            
            # 기간 타입 설정
            if period_type == 'annual':
                objRq3.SetInputValue(1, ord('A'))  # 연간
            elif period_type == 'quarter':
                objRq3.SetInputValue(1, ord('Q'))  # 분기별
            else:
                objRq3.SetInputValue(1, ord('A'))  # 기본값: 연간
                
            objRq3.SetInputValue(2, count)
            objRq3.BlockRequest()
            
            # ROE, 부채비율 데이터 추가
            for i in range(len(financial_data)):
                if i < objRq3.GetHeaderValue(0):
                    roe = objRq3.GetDataValue(4, i)        # ROE
                    debt_ratio = objRq3.GetDataValue(5, i)  # 부채비율
                    
                    financial_data[i]['roe'] = roe
                    financial_data[i]['debt_ratio'] = debt_ratio
            
            # 현재 PER, PBR 추가 (최신 데이터에)
            if financial_data:
                financial_data[0]['per'] = per
                financial_data[0]['pbr'] = pbr
            
            # 최종 DataFrame 생성 및 반환
            df = pd.DataFrame(financial_data)
            
            # 데이터가 있을 경우 처리
            if not df.empty:
                # 기간 타입에 따른 컬럼명 설정
                if period_type == 'quarter':
                    df.rename(columns={'period': 'quarter'}, inplace=True)
                else:
                    df.rename(columns={'period': 'year'}, inplace=True)
            
            return df
            
        except Exception as e:
            print(f"재무데이터 조회 중 오류: {formatted_code} - {str(e)}")
            # 대체 방법 시도 (다른 COM 객체 사용)
            return self.get_financial_data_alternative(formatted_code, period_type, count)
    
    def get_financial_data_alternative(self, stock_code, period_type='annual', count=5):
        """
        대체 방법으로 재무데이터 조회 (다른 COM 객체 사용)
        """
        try:
            financial_data = []
            
            # 기본적인 재무 정보만 조회
            objRq = win32com.client.Dispatch("CpSysDib.StockMst")
            objRq.SetInputValue(0, stock_code)
            objRq.BlockRequest()
            
            # 현재 정보만 가져오기
            current_price = objRq.GetHeaderValue(11) if objRq.GetHeaderValue(11) else 0
            per = objRq.GetHeaderValue(12) if objRq.GetHeaderValue(12) else 0
            pbr = 0  # 기본값
            
            # 임시 데이터 구조 (실제 재무데이터는 다른 방법으로 조회 필요)
            current_year = datetime.datetime.now().year
            for i in range(count):
                year_or_quarter = current_year - i if period_type == 'annual' else f"{current_year}Q{4-i}"
                
                financial_data.append({
                    'period': year_or_quarter,
                    'revenue': 0,
                    'operating_profit': 0,
                    'net_profit': 0,
                    'per': per if i == 0 else 0,
                    'pbr': pbr,
                    'roe': 0,
                    'debt_ratio': 0
                })
            
            df = pd.DataFrame(financial_data)
            
            if not df.empty:
                if period_type == 'quarter':
                    df.rename(columns={'period': 'quarter'}, inplace=True)
                else:
                    df.rename(columns={'period': 'year'}, inplace=True)
            
            return df
            
        except Exception as e:
            print(f"대체 방법으로도 조회 실패: {stock_code} - {str(e)}")
            return pd.DataFrame()

    def get_comprehensive_financial_data(self, stock_code, period_type='annual', count=5):
        """
        포괄적인 재무데이터 조회
        :param stock_code: 종목 코드
        :param period_type: 'annual' (연간) 또는 'quarter' (분기별)
        :param count: 조회할 기간 개수
        :return: Pandas DataFrame
        """
        try:
            # 기본 정보
            formatted_code = self.format_stock_code(stock_code)
            stock_name = self.objCpCodeMgr.CodeToName(formatted_code)
            
            print(f"종목: {stock_name} ({formatted_code}) 재무데이터 조회 중...")
            
            # 재무데이터 조회
            df = self.get_financial_data(formatted_code, period_type, count)
            
            # 데이터 정리
            if not df.empty:
                df['stock_code'] = formatted_code
                df['stock_name'] = stock_name
                
                # 컬럼 순서 조정 (기간 타입에 따라)
                period_col = 'year' if period_type == 'annual' else 'quarter'
                columns = ['stock_code', 'stock_name', period_col, 'revenue', 
                          'operating_profit', 'net_profit', 'per', 'pbr', 
                          'roe', 'debt_ratio']
                
                # 존재하는 컬럼만 선택
                existing_columns = [col for col in columns if col in df.columns]
                df = df[existing_columns]
                
                # 결측값 처리
                df = df.fillna(0)
            
            time.sleep(0.5)  # API 호출 간격 조절
            return df
            
        except Exception as e:
            formatted_code = self.format_stock_code(stock_code)
            print(f"오류 발생: {formatted_code} - {str(e)}")
            return pd.DataFrame()
    
    def get_multiple_stocks_data(self, stock_codes, period_type='annual', count=5):
        """
        여러 종목의 재무데이터 조회
        :param stock_codes: 종목 코드 리스트
        :param period_type: 'annual' (연간) 또는 'quarter' (분기별)
        :param count: 조회할 기간 개수
        :return: Pandas DataFrame
        """
        all_data = []
        
        for stock_code in stock_codes:
            df = self.get_comprehensive_financial_data(stock_code, period_type, count)
            if not df.empty:
                all_data.append(df)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()

def main():
    """메인 실행 함수"""
    # CREON API 초기화
    creon = CreonFinancialData()
    
    # 예시: 삼성전자, SK하이닉스, NAVER 재무데이터 조회 (A 없이 입력)
    stock_codes = ['005930', '000660', '035420']  # 삼성전자, SK하이닉스, NAVER
    
    # 재무데이터 조회 (최근 5년 연간)
    financial_df = creon.get_multiple_stocks_data(stock_codes, period_type='annual', count=5)
    
    print("\n=== 연간 재무데이터 조회 결과 ===")
    if not financial_df.empty:
        print(financial_df.to_string(index=False))
        
        # CSV 파일로 저장
        today = datetime.datetime.now().strftime("%Y%m%d")
        filename = f"annual_financial_data_{today}.csv"
        financial_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n연간 데이터가 {filename}에 저장되었습니다.")
    
    # 분기별 데이터도 조회 (최근 8분기)
    quarterly_df = creon.get_multiple_stocks_data(stock_codes, period_type='quarter', count=8)
    
    print("\n=== 분기별 재무데이터 조회 결과 ===")
    if not quarterly_df.empty:
        print(quarterly_df.to_string(index=False))
        
        # CSV 파일로 저장
        filename_q = f"quarterly_financial_data_{today}.csv"
        quarterly_df.to_csv(filename_q, index=False, encoding='utf-8-sig')
        print(f"\n분기별 데이터가 {filename_q}에 저장되었습니다.")
    
    if not financial_df.empty:
        # 기본 통계 (연간 데이터)
        print("\n=== 연간 데이터 기본 통계 ===")
        numeric_columns = ['revenue', 'operating_profit', 'net_profit', 
                          'per', 'pbr', 'roe', 'debt_ratio']
        existing_numeric_cols = [col for col in numeric_columns if col in financial_df.columns]
        if existing_numeric_cols:
            print(financial_df[existing_numeric_cols].describe())
    
    if not quarterly_df.empty:
        # 기본 통계 (분기별 데이터)
        print("\n=== 분기별 데이터 기본 통계 ===")
        numeric_columns = ['revenue', 'operating_profit', 'net_profit']
        existing_numeric_cols = [col for col in numeric_columns if col in quarterly_df.columns]
        if existing_numeric_cols:
            print(quarterly_df[existing_numeric_cols].describe())
    
    if financial_df.empty and quarterly_df.empty:
        print("데이터를 가져올 수 없습니다.")

# 개별 종목 조회 예시
def get_single_stock_example():
    """단일 종목 조회 예시"""
    creon = CreonFinancialData()
    
    # 삼성전자 연간 재무데이터 조회 (최근 3년) - A 없이 입력
    stock_code = '005930'
    df_annual = creon.get_comprehensive_financial_data(stock_code, period_type='annual', count=3)
    
    if not df_annual.empty:
        print(f"\n{df_annual.iloc[0]['stock_name']} 연간 재무데이터:")
        period_col = 'year' if 'year' in df_annual.columns else 'period'
        for _, row in df_annual.iterrows():
            print(f"년도: {row[period_col]}")
            print(f"매출액: {row['revenue']:,}백만원")
            print(f"영업이익: {row['operating_profit']:,}백만원")
            print(f"당기순이익: {row['net_profit']:,}백만원")
            if 'per' in row and row['per'] != 0:
                print(f"PER: {row['per']:.2f}")
            if 'pbr' in row and row['pbr'] != 0:
                print(f"PBR: {row['pbr']:.2f}")
            if 'roe' in row and row['roe'] != 0:
                print(f"ROE: {row['roe']:.2f}%")
            if 'debt_ratio' in row and row['debt_ratio'] != 0:
                print(f"부채비율: {row['debt_ratio']:.2f}%")
            print("-" * 40)
    
    # 분기별 데이터도 조회 (최근 4분기)
    df_quarter = creon.get_comprehensive_financial_data(stock_code, period_type='quarter', count=4)
    
    if not df_quarter.empty:
        print(f"\n{df_quarter.iloc[0]['stock_name']} 분기별 재무데이터:")
        period_col = 'quarter' if 'quarter' in df_quarter.columns else 'period'
        for _, row in df_quarter.iterrows():
            print(f"분기: {row[period_col]}")
            print(f"매출액: {row['revenue']:,}백만원")
            print(f"영업이익: {row['operating_profit']:,}백만원")
            print(f"당기순이익: {row['net_profit']:,}백만원")
            print("-" * 40)

if __name__ == "__main__":
    main()
    # get_single_stock_example()  # 단일 종목 예시 실행