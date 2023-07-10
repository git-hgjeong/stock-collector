import sys
from PyQt5.QAxContainer import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
import pandas as pd
from io import StringIO
import time
import requests
import json

is_64bits = sys.maxsize > 2**32
if is_64bits:
    print('64bit 환경입니다.')
else:
    print('32bit 환경입니다.')

class KiwoomAPI(QAxWidget):
    # 생성자
    def __init__(self):
        super().__init__()

        # QAxWidget 객체 (키움 api연결)
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")
        self.login_event_loop = QEventLoop()    # login event loop 생성

        # 로그인 콜백함수 지정
        self.OnEventConnect.connect(self.apiOnEventConnect)

        # 로그인
        print("- Login Start -")
        self.dynamicCall("CommConnect()")
        self.login_event_loop.exec_()   # login event loop 시작

        # 데이터요청(CommRqData) 콜백함수 지정
        self.DATA_StockList = []
        self.DATA_DayTradeList = []
        self.DATA_DayProgramData = []
        self.OnReceiveTrData.connect(self.apiOnReceiveTrData)

        # 조건식 콜백함수 지정
        self.OnReceiveTrCondition.connect(self.apiOnReceiveTrCondition)
        self.OnReceiveConditionVer.connect(self.apiOnReceiveConditionVer)

        # 조건식에 해당하는 종목코드 조회결과 데이터
        self.DATA_StockCodeList = []

    # 로그인 콜백함수
    def apiOnEventConnect(self, nErrCode):
        if nErrCode == 0:
            print('- Login Success -')
        else:
            print('- Login Fail -')
        self.login_event_loop.exit()    # login event loop 종료

    # 데이터요청(CommRqData) 콜백함수
    # sScrNo(화면번호), sRQName(사용자구분), sTrCode(Tran명), sRecordName(레코드명), sPreNext(연속조회 유무)
    def apiOnReceiveTrData(self, sScrNo, sRQName, sTrCode, sRecordName, sPreNext):
        #print(">> Receive Result:"+ sRQName)
        if sRQName == 'GET-BASIC-DATA':
            # 종목명	현재가	등락률	거래량	거래대금(백만)
            name = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "종목코드")
            price = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "종목명")
            rate = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "등락율")
            per = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "PER")
            tp = (name.lstrip(), price.lstrip(), rate.lstrip(), per.lstrip())
            print(tp)
            self.DATA_StockList.append(tp)

        elif sRQName == 'GET-DAY-DATA':
            code = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "종목코드")
            price = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "현재가")
            volume = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "거래량")
            trade_price = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "거래대금")
            date = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "일자")
            #start_price = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "시가")
            #high_price = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "고가")
            #low_price = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "저가")
            tp = (code.lstrip(), price.lstrip(), volume.lstrip(), trade_price.lstrip(), date.lstrip())
            #print(tp)
            self.DATA_DayTradeList.append(tp)
        elif sRQName.find('GET-PROGRAM-DATA') == 0:
            ticker = sRQName[sRQName.find('_')+1:]
            date = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "일자")
            buy_count = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "프로그램매도수량")
            sell_count = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "프로그램매수수량")
            count = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0,
                                          "프로그램순매수수량증감")
            # buy_amount = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "프로그램매도금액")
            # sell_amount = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "프로그램매수금액")

            tp = (ticker, date.lstrip(), buy_count.lstrip(), sell_count.lstrip(), count.lstrip())
            print(tp)
            self.DATA_DayProgramData.append(tp)

        self.data_event_loop.exit()

    def apiOnReceiveTrCondition(self, scrno, codelist, conditionname, nindex, nnext):
        print(">> apiOnReceiveTrCondition ")
        print(codelist)
        if codelist:
            arr = codelist.split(';')
            arr.pop()
            self.DATA_StockCodeList = arr
            print(self.DATA_StockCodeList)
            print(">>>> 조건식 종목: ", len(self.DATA_StockCodeList), " 개 확인.")
            #print(arr)
        else:
            print("#### 조건식에 해당하는 종목이 없습니다. ####")

        self.data_event_loop.exit()
    def apiOnReceiveConditionVer(self):
        #print(">> 조건식 데이터 조회 완료")
        sConditionResult = self.dynamicCall("GetConditionNameList()")
        print(">>>> 조건식:"+ sConditionResult)
        # 세미콜론과 캐럿으로 구성된 CSV 형식으로 변환.
        csv_str = sConditionResult.replace(';', '\n').replace('^', ',')
        # 문자열을 DataFrame으로 변환.
        df = pd.read_csv(StringIO(csv_str), header=None, dtype=object)
        # DataFrame column header 설정
        df.columns = ["code", "name"]
        #print(df)
        self.DATA_Conditions = df
        self.data_event_loop.exit()
    def getBasicData(self, stock_code):
        # 연속 요청시 씹히는 문제 sleep 처리
        time.sleep(0.2)
        #print(">> 데이터요청:"+ stock_code)
        # 검색조건
        self.dynamicCall("SetInputValue(QString, QString)", "종목코드", stock_code)
        # 데이터 조회 실행
        self.dynamicCall("CommRqData(QString, QString, QString, QString)", "GET-BASIC-DATA", "opt10001", "0", "0101")
        self.data_event_loop = QEventLoop()
        self.data_event_loop.exec_()

    def getDayData(self, stock_code):
        # 연속 요청시 씹히는 문제 sleep 처리
        time.sleep(0.2)
        #print(">> 데이터요청:"+ stock_code)
        # 검색조건
        self.dynamicCall("SetInputValue(QString, QString)", "종목코드", stock_code)
        self.dynamicCall("SetInputValue(QString, QString)", "기준일자", None)
        self.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", 1)
        # 데이터 조회 실행
        self.dynamicCall("CommRqData(QString, QString, QString, QString)", "GET-DAY-DATA", "opt10081", "0", "0101")
        self.data_event_loop = QEventLoop()
        self.data_event_loop.exec_()

    def getDayProgramData(self, stock_code):
        # 연속 요청시 씹히는 문제 sleep 처리
        time.sleep(0.2)
        #print(">> 데이터요청:"+ stock_code)
        # 검색조건
        self.dynamicCall("SetInputValue(QString, QString)", "시간일자구분", "2")
        self.dynamicCall("SetInputValue(QString, QString)", "금액수량구분", "2") # 1:금액, 2:수량
        self.dynamicCall("SetInputValue(QString, QString)", "종목코드", stock_code)
        self.dynamicCall("SetInputValue(QString, QString)", "날짜", None)
        # 데이터 조회 실행
        self.dynamicCall("CommRqData(QString, QString, QString, QString)", "GET-PROGRAM-DATA_"+ stock_code, "opt90013", "0", "0101")
        self.data_event_loop = QEventLoop()
        self.data_event_loop.exec_()

    def getMyConditions(self):
        self.dynamicCall("GetConditionLoad()")
        self.data_event_loop = QEventLoop()
        self.data_event_loop.exec_()

    def getMyConditionData(self, sConditionName):
        self.DATA_StockList = []
        self.getMyConditions()
        is_filter = self.DATA_Conditions['name'] == sConditionName
        df = self.DATA_Conditions[is_filter]
        code = df.at[0, 'code']
        is_result = self.dynamicCall("SendCondition(QString, QString, int, int)", "0156", sConditionName, code, 0)
        #print(is_result)
        if is_result == 1:
            self.data_event_loop = QEventLoop()
            self.data_event_loop.exec_()

            print(self.DATA_StockCodeList)
            for code in self.DATA_StockCodeList:
                #print(code)
                self.getBasicData(code)
                self.getDayData(code)
                self.getDayProgramData(code)
            #print("==========================================")
            dfStock = pd.DataFrame(self.DATA_StockList, columns=['ticker','stock_name', 'change_price_rate', 'per'])
            #print(dfStock)
            dfDayTrade = pd.DataFrame(self.DATA_DayTradeList, columns=['ticker', 'price', 'trading_volume', 'trading_amount', 'trading_date'])
            #print(dfDayTrade)
            dfDayProgramData = pd.DataFrame(self.DATA_DayProgramData,
                                      columns=['ticker', 'program_trading_date', 'program_buy_count', 'program_sell_count', 'program_count'])
            # print(dfDayProgramData)

            # data join
            dfMergeData1 = pd.merge(dfStock, dfDayTrade, left_on='ticker', right_on='ticker', how='outer')
            # print(dfMergeData1)
            dfMergeData = pd.merge(dfMergeData1, dfDayProgramData, left_on='ticker', right_on='ticker', how='outer')
            # print(dfMergeData)
            if dfMergeData.size > 0:
                dfMergeData.to_excel(dfMergeData.at[0, 'trading_date'] +'.xlsx')
            else:
                print("#### Data not found. ####")

        else:
            print("Fail getMyConditionData.")
        # Transfer to API Server
        if dfMergeData.size > 0:
            self.addStockDataToApiServer(dfMergeData)

    def addStockDataToApiServer(self, dfMergeData):
        requestData = dfMergeData.to_json(orient='records')
        # URL
        url = "http://dev592.cafe24.com/stock/api/add.php"
        # headers
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(url, headers=headers, data=requestData)
        print("response: ", response)
        print("response.text: ", response.text)
        url = "https://dev592.cafe24.com/stock/attentions.php?from_date="+ dfMergeData.at[0, 'trading_date'] +"&to_date="+ dfMergeData.at[0, 'trading_date']
        print(url)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    kiwwom = KiwoomAPI()
    #kiwwom.getMyConditions()
    #kiwwom.getBasicData("005385")
    #kiwwom.getDayData("005385")
    #kiwwom.getDayProgramData("123570")
    kiwwom.getMyConditionData('기본')