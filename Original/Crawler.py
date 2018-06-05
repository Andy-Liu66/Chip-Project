#此部分為一開始未使用OOP的版本，當初是以爬取上市公司資料為目標
import requests
import json
import pandas as pd
import numpy as np
import datetime
import time
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

#request.get裡面要加 headers={'Connection':'close'} 才不會出現"Failed to establish a new connection"
#若沒有交易紀錄則無資料，但法人沒交易紀錄但融資融券可能有→要處理，依據日期index合併資料

#三大法人買賣超
def get_main_force(stocktype, date):
    url = 'http://www.twse.com.tw/fund/T86'
    params = {}
    params['selectType'] = stocktype #產業類別
    params['date'] = date
    params['response'] = json

    res = requests.get(url, params=params, headers={'Connection':'close'})
    s = json.loads(res.text)
    
    try:
        rawData=pd.DataFrame(s['data'],columns=['證券代號','證券名稱','外陸資買進股數(不含外資自營商)','外陸資賣出股數(不含外資自營商)',
                                     '外陸資買賣超股數(不含外資自營商)','外資自營商買進股數','外資自營商賣出股數','外資自營商買賣超股數',
                                     '投信買進股數','投信賣出股數','投信買賣超股數','自營商買賣超股數','自營商買進股數(自行買賣)','自營商賣出股數(自行買賣)',
                                     '自營商買賣超股數(自行買賣)','自營商買進股數(避險)','自營商賣出股數(避險)','自營商買賣超股數(避險)','三大法人買賣超股數'])
        
        rawData["日期"] = datetime.datetime.strptime("{}".format(date), '%Y%m%d')
        
    except:
        rawData = pd.DataFrame([])
    
    return rawData


#融資融券
def get_margin_purchase_short_sell(stock_type, date):
    url = 'http://www.twse.com.tw/exchangeReport/MI_MARGN'
    params = {}
    params['selectType'] = stock_type
    params['date'] = date
    params['response'] = json
    
    res = requests.get(url, params=params, headers={'Connection':'close'})
    s = json.loads(res.text)
    
    try:
        rawData=pd.DataFrame(s['data'],columns=s['fields'])
        
        rawData["日期"] = datetime.datetime.strptime("{}".format(date), '%Y%m%d')
        
    except:
        rawData = pd.DataFrame([])
        
    return rawData


#借券
def get_borrow(stock_type, date):
    url = 'http://www.twse.com.tw/exchangeReport/TWT93U.html'
    params = {}
    params['selectType'] = stock_type
    params['date'] = date
    params['response'] = json
    
    res = requests.get(url, params=params, headers={'Connection':'close'})
    s = json.loads(res.text)
    
    try:
        rawData=pd.DataFrame(s['data'],columns=s['fields'])
        
        rawData["日期"] = datetime.datetime.strptime("{}".format(date), '%Y%m%d')
        
    except:
        rawData = pd.DataFrame([])
        
    return rawData


#抓取並整理每日資料
def get_today_data(date = int(str(datetime.datetime.today())[0:10].replace("-", ""))):
    
    try:
        #處理三大法人買賣超
        today_main_force = get_main_force("ALLBUT0999", date)[["日期", "證券代號", "證券名稱", "外陸資買賣超股數(不含外資自營商)", "投信買賣超股數"]]
        today_main_force = today_main_force[today_main_force.證券代號.apply(lambda x : len(x) == 4)]
        today_main_force.columns = ["日期", "股票代號", "股票名稱", "外陸資買賣超股數(不含外資自營商)", "投信買賣超股數"]
        today_main_force.iloc[:, [3, 4]] = today_main_force.iloc[:, [3, 4]].applymap(lambda x : float(x.replace(",", ""))) / 1000

        #處理融資融券
        today_margin_purchase_short_sell = get_margin_purchase_short_sell("ALL", date)
        today_margin_purchase_short_sell = today_margin_purchase_short_sell[today_margin_purchase_short_sell.股票代號.apply(lambda x : len(x) == 4)]
        today_margin_purchase_short_sell.iloc[:, [5, 6, 11, 12]] = today_margin_purchase_short_sell.iloc[:, [5, 6, 11, 12]].applymap(lambda x : float(x.replace(",", "")))
        today_margin_purchase_short_sell["融資張數"] = today_margin_purchase_short_sell.iloc[:, 6] - today_margin_purchase_short_sell.iloc[:, 5]
        today_margin_purchase_short_sell["融券張數"] = today_margin_purchase_short_sell.iloc[:, 12] - today_margin_purchase_short_sell.iloc[:, 11]
        today_margin_purchase_short_sell = today_margin_purchase_short_sell[["日期", '股票代號', '股票名稱', "融資張數", "融券張數"]]

        #處理借券
        today_borrow = get_borrow("ALL", date).iloc[:, [15, 0, 1, 8, 12]]
        today_borrow = today_borrow[today_borrow.股票代號.apply(lambda x : len(str(x)) == 4)]
        today_borrow.iloc[:, [3, 4]] = today_borrow.iloc[:, [3, 4]].applymap(lambda x : float(x.replace(",", "")))
        today_borrow["借券張數"] = (today_borrow.iloc[:, 4] - today_borrow.iloc[:, 3]) / 1000
        today_borrow = today_borrow[['日期', '股票代號', '股票名稱', "借券張數"]]

        #處理全部
        today_data = today_main_force.merge(today_margin_purchase_short_sell, on = ["日期", "股票代號"], how = "outer").merge(today_borrow, on = ["日期", "股票代號"], how = "outer")
        today_data = today_data[['日期', '股票代號', '股票名稱', '外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '借券張數', '融資張數', '融券張數']]
        today_data.股票代號 = today_data.股票代號.apply(lambda x : int(x)) #由於累積資料集在寫入excel檔時會自動變成int，所以這裡也要更改
        today_data.日期 = today_data.日期.apply(lambda x : str(x)[0:10])
        
        return today_data 
    
    except:
        #之後這裡寄信報錯
        return pd.DataFrame()


#合併新抓取之資料與累積資料集並計算自定義指標
def calculate_indicator(individual_data, temp_result, outstanding_stock):

    # #將temp_result格式轉成datetime
    # temp_result.日期 = temp_result.日期.apply(lambda x : datetime.datetime.strptime(x, '%Y-%m-%d'))
    # #將individual_data格式轉成datetime
    # individual_data.日期 = individual_data.日期.apply(lambda x : datetime.datetime.strptime(x, '%Y-%m-%d'))
    #建立輸入資料之日期集合
    local_time_set = set(individual_data.日期)
    #建立所有交易日之日期集合
    global_time_set = set(temp_result.日期.unique())
    #取兩集合之差集
    temp_output = pd.DataFrame(0, index = np.arange(len(global_time_set.difference(local_time_set))), columns = temp_result.columns)
    
    #若有差集則代表此個股並非所有交易日皆有相關操作資料，因此需要補空值0(由於之後需要用到累加的資料，補植後才不會失真)
    #有缺值則補植
    if len(global_time_set.difference(local_time_set)) > 0:
        temp_output["日期"] = global_time_set.difference(local_time_set)
        temp_output["股票代號"] = individual_data["股票代號"].unique()[0]
        temp_output["股票名稱"] = individual_data["股票名稱"].unique()[0]
        individual_data = pd.concat([individual_data, temp_output]).sort_values("日期")
    #沒缺值則不補(會加上空的Dataframe)
    else:
        individual_data = pd.concat([individual_data, temp_output]).sort_values("日期")


    #個股資料依據日期整理完善後，計算自定義的籌碼指標
    individual_data["當日指標餘額"] = (individual_data["外陸資買賣超股數(不含外資自營商)"] + individual_data["投信買賣超股數"] - individual_data["借券張數"]) - (individual_data["融資張數"] - individual_data["融券張數"])


    #接著將計算出的指標除上流通在外淨股數(計算方式為流通在外股數減去董監持股，依據2018/05/15由TEJ抓取的資料，因此並非持續更新的資料，且未扣除庫藏股)
    #有些公司可能下市因此首先檢查是否在清單中有此公司(因為可能清單沒有，但爬下來的歷史資料中有此公司)，若下市則直接將此公司資料清空
    if len(outstanding_stock[outstanding_stock.股票代號 == individual_data.股票代號.unique()[0]]) == 0:
        code = individual_data["股票代號"].unique()[0]
        individual_data = pd.DataFrame(0, index = np.arange(1), columns = temp_result.columns)
        individual_data["股票代號"] = code
        individual_data["股票名稱"] = "此檔股票為ETF或已下市"
    #若有資料則計算指標
    else:
        outstanding = outstanding_stock[outstanding_stock.股票代號 == individual_data.股票代號.unique()[0]].淨流通在外張數.values[0]
        individual_data.fillna(0, inplace = True)
        individual_data["近5日指標比例"] = individual_data["當日指標餘額"].rolling(5).sum() / outstanding
        individual_data["近10日指標比例"] = individual_data["當日指標餘額"].rolling(10).sum() / outstanding
        individual_data["近20日指標比例"] = individual_data["當日指標餘額"].rolling(20).sum() / outstanding
        individual_data["近60日指標比例"] = individual_data["當日指標餘額"].rolling(60).sum() / outstanding
        individual_data["股票名稱"] = outstanding_stock[outstanding_stock.股票代號 == individual_data.股票代號.unique()[0]].股票名稱.values[0]
    
    #最終回傳計算結果(最新一筆資料)
    return pd.DataFrame(individual_data.iloc[-1, :]).T


#將計算結果以email寄出
class gmail:
    
    def __init__(self, receiver, result_table):
        self.my_mail = "andy566159@gmail.com"
        self.receiver = receiver
        self.smtplib_object = smtplib.SMTP('smtp.gmail.com', 587)
        self.smtplib_object.starttls()
        self.smtplib_object.login(self.my_mail, os.environ["gmail_pwd"])
        self.receiver = receiver
        self.result_table = result_table
        
    def send_gmail(self):
        msg = MIMEMultipart('alternative')
        msg["Subject"] = "{} 籌碼指標".format(str(self.result_table.日期.unique())[2:12])
        msg["From"] = self.my_mail
        msg["to"] = self.receiver
        html = self.result_table.iloc[:, 1:].to_html()
        html_part = MIMEText(html, 'html')
        msg.attach(html_part)
        self.smtplib_object.sendmail(self.my_mail,self.receiver, msg.as_string())
        self.smtplib_object.quit()


# 每日自動化部分(最後未採用)
# #讀進資料
# temp_result = pd.read_excel("2018_temp_result.xlsx")
# outstanding_stock = pd.read_excel("2018_董監持股_流通張數.xlsx")

# #抓取今天資料
# today_data = get_today_data() #若不填入資料則預設為今日日期

# #合併回累積資料集
# temp_result = pd.concat([temp_result, today_data])

# #去除重複抓取值(以防萬一)
# temp_result.drop_duplicates(inplace = True)

# #寫入(回)累積資料集(迭代)
# temp_result.to_excel("2018_temp_result.xlsx")

# #計算自定義籌碼指標
# result_table = temp_result.groupby("股票代號", as_index = False, group_keys = False).apply(calculate_indicator)

# #指定計算結果的欄位順序
# result_table = result_table[["日期", "股票代號", "股票名稱", "外陸資買賣超股數(不含外資自營商)", "投信買賣超股數", "借券張數",
#                              "融資張數", "融券張數", "當日指標餘額", "近5日指標比例", "近10日指標比例", "近20指標比例", "近60日指標比例"]]

# #依據近五日指標比例排序
# result_table = result_table.sort_values("近5日指標比例", ascending = False).reset_index(drop = True)