import pandas as pd
import requests
import json
from datetime import datetime
from Exchange import deal_with_data

##基本架構
class OTC:

    def __init__(self, date):
        transform_date = str(int(date / 10000) - 1911) + "/" + str(date)[4:6] + "/" + str(date)[6:8]
        #保留原先date格式之後會用到
        self.date = date 
        #輸入requests時需要用「民國」格式
        self.transform_date = transform_date
        self.params = {}
        self.params['d'] = self.transform_date
        #None將透過繼承填入
        self.url = None 
        self.selected_columns = None
        #columns由於格式問題無法直接由爬下來的url_data獲得，因此改用手動輸入
        self.url_data_columns = None 

    def get_original_data(self):
        self.url_data = requests.get(self.url, params = self.params, headers = {'Connection':'close'})
        self.url_data = json.loads(self.url_data.text)
        #由於爬下來的原始資料最後一欄為空值，因此要去除此欄(不選擇此欄)
        try:
            self.url_data = pd.DataFrame(self.url_data['aaData'])
            self.url_data = self.url_data.iloc[:, range(len(self.url_data.columns) - 1)]
            self.url_data.columns = self.url_data_columns
            self.url_data["日期"] = datetime.strptime("{}".format(self.date), '%Y%m%d')  
        except:
            self.url_data = pd.DataFrame([])   
        return self.url_data


class OTC_main_force(OTC):
    def __init__(self, date):
        OTC.__init__(self, date)
        self.url = "http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=EW&t=D&"
        self.selected_columns = ["日期", "股票代號", "股票名稱", "外陸資買賣超股數(不含外資自營商)", "投信買賣超股數"]
        self.url_data_columns = ['股票代號', '股票名稱',
                                 '外陸資買進股數(不含外資自營商)','外陸資賣出股數(不含外資自營商)','外陸資買賣超股數(不含外資自營商)',
                                 '外資自營商買進股數','外資自營商賣出股數','外資自營商買賣超股數',
                                 '外陸資買進股數','外陸資賣出股數','外陸資買賣超股數',
                                 '投信買進股數','投信賣出股數','投信買賣超股數',
                                 '自營商買進股數(自行買賣)','自營商賣出股數(自行買賣)', '自營商買賣超股數(自行買賣)',
                                 '自營商買進股數(避險)','自營商賣出股數(避險)','自營商買賣超股數(避險)',
                                 '自營商買進股數','自營商賣出股數','自營商買賣超股數','三大法人買賣超股數'] 
        
    def get_selected_data(self):
        try:
            #抓出要的資料
            self.selected_data = self.get_original_data()[self.selected_columns].copy()
            self.selected_data.股票代號 = self.selected_data.股票代號.apply(lambda x : x.split(" ")[0])
            #只選代號為四碼者(排除選擇權、債券...)
            self.selected_data = self.selected_data[self.selected_data.股票代號.apply(lambda x : len(x) == 4)] 
            #去除","
            self.selected_data.iloc[:, [3, 4]] = self.selected_data.iloc[:, [3, 4]].applymap(lambda x : float(x.replace(",", ""))) / 1000 
            #統一字串格式之後merge會影響
            self.selected_data.股票名稱 = self.selected_data.股票名稱.apply(lambda x : x.split(" ")[0])
        except:
            self.selected_data = pd.DataFrame([])
        return self.selected_data


class OTC_margin_purchase_short_sell(OTC):
    def __init__(self, date):
        OTC.__init__(self, date)
        self.url = "http://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&"
        self.selected_columns = ["日期", "股票代號", "股票名稱", "融資張數", "融券張數"]
        self.url_data_columns = ["股票代號", "股票名稱", 
                                 "前資餘額(張)", "資買", "資賣", "現債", "資餘額", "資屬證金", "資使用率(%)", "資限額",
                                 "前券餘額(張)", "券賣", "券買", "券償", "券餘額", "券屬證金", "券使用率(%)", "券限額", "資券相抵(張)"]
    def get_selected_data(self):
        try:
            self.selected_data = self.get_original_data().copy()
            self.selected_data = self.selected_data[self.selected_data.股票代號.apply(lambda x : len(x) == 4)]
            self.selected_data.iloc[:, [2, 6, 10 ,14]] = self.selected_data.iloc[:, [2, 6, 10 ,14]].applymap(lambda x : float(x.replace(",", "")))
            #計算融資張數
            self.selected_data["融資張數"] = self.selected_data.iloc[:, 6] - self.selected_data.iloc[:, 2] 
            #計算融券張數
            self.selected_data["融券張數"] = self.selected_data.iloc[:, 14] - self.selected_data.iloc[:, 10] 
            self.selected_data = self.selected_data[self.selected_columns]
        except:
            self.selected_data = pd.DataFrame([])
        return self.selected_data


class OTC_borrow(OTC):
    def __init__(self, date):
        OTC.__init__(self, date)
        self.url = "http://www.tpex.org.tw/web/stock/margin_trading/margin_sbl/margin_sbl_result.php?l=zh-tw&"
        self.selected_columns = ["日期", "股票代號", "股票名稱", "借券張數"]
        self.url_data_columns = ["股票代號", "股票名稱",
                                 "融券前日餘額", "融券賣出", "融券買進", "融券現券", "融券今日餘額", "融券限額", 
                                 "借券前日餘額", "借券當日賣出", "借券當日還券", "借券當日調整數額", "借券當日餘額", "今日可借券限額"]
    
    def get_selected_data(self):
        try:
            #選擇後半借券的部分
            self.selected_data = self.get_original_data().iloc[:, [14, 0, 1, 8, 12]].copy() 
            self.selected_data = self.selected_data[self.selected_data.股票代號.apply(lambda x : len(str(x)) == 4)]
            self.selected_data.iloc[:, [3, 4]] = self.selected_data.iloc[:, [3, 4]].applymap(lambda x : float(x.replace(",", "")))
            self.selected_data["借券張數"] = (self.selected_data.iloc[:, 4] - self.selected_data.iloc[:, 3]) / 1000
            self.selected_data = self.selected_data[['日期', '股票代號', '股票名稱', "借券張數"]]
        except:
            self.selected_data = pd.DataFrame([])
        return self.selected_data


class OTC_deal_with_data(deal_with_data):

    def __init__(self, date = []):
        if date == []:
            self.date = int(str(datetime.today())[0:10].replace("-", ""))
        else:
            self.date = date
        self.main_force = OTC_main_force(self.date).get_selected_data()
        self.margin_purchase_short_sell = OTC_margin_purchase_short_sell(self.date).get_selected_data()
        self.borrow = OTC_borrow(self.date).get_selected_data()
        self.cumulative_data = "2018_OTC_temp_result.csv"
        self.outstanding_stock = "2018_outstanding_stock.csv"
        self.type = "OTC"
        self.market = "上櫃公司"