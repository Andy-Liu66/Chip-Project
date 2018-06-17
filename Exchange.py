import pandas as pd
import numpy as np
from datetime import datetime
import requests
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


##基本架構
class Exchange:

    def __init__(self, date, stock_type):
        self.date = date
        self.stock_type = stock_type
        self.params = {}
        self.params['selectType'] = self.stock_type
        self.params['date'] = self.date
        #None將透過繼承填入
        self.url = None
        self.selected_columns = None

    def get_original_data(self):
        self.url_data = requests.get(self.url, params = self.params, headers = {'Connection':'close'})
        self.url_data = json.loads(self.url_data.text)
        try:
            self.url_data = pd.DataFrame(self.url_data['data'], columns = self.url_data['fields'])
            self.url_data["日期"] = datetime.strptime("{}".format(self.date), '%Y%m%d')      
        except:
            self.url_data = pd.DataFrame([])  
        return self.url_data


##繼承自基本架構
#三大法人買賣超
class Exchange_main_force(Exchange):

    def __init__(self, date, stock_type = "ALL"):
        Exchange.__init__(self, date, stock_type)
        self.url = "http://www.tse.com.tw/fund/T86?response=json&"
        #指定要的資料欄位，與網頁上相同
        self.selected_columns = ["日期", "證券代號", "證券名稱", "外陸資買賣超股數(不含外資自營商)", "投信買賣超股數"] 

    def get_selected_data(self):
        try:
            #抓出要的資料
            self.selected_data = self.get_original_data()[self.selected_columns] 
            #只選代號為四碼者(排除選擇權、債券...)
            self.selected_data = self.selected_data.copy()[self.selected_data.證券代號.apply(lambda x : len(x) == 4)]
            #更改欄位名稱使其與之後處理之資料一致
            self.selected_data.columns.values[[1, 2]] = ["股票代號", "股票名稱"]
            #去除","
            self.selected_data.iloc[:, [3, 4]] = self.selected_data.iloc[:, [3, 4]].applymap(lambda x : float(x.replace(",", ""))) / 1000
            #統一字串格式之後merge會影響
            self.selected_data.股票名稱 = self.selected_data.股票名稱.apply(lambda x : x.split(" ")[0])
        except:
            self.selected_data = pd.DataFrame([])
        return self.selected_data
#若沒交易日會回傳「很抱歉，沒有符合條件的資料!」 → 會進到except
#若日期未到會回傳「查詢日期大於可查詢最大日期，請重新查詢!」 → 會進到except


#融資融券
class Exchange_margin_purchase_short_sell(Exchange):
    def __init__(self, date, stock_type = "ALL"):
        Exchange.__init__(self, date, stock_type)
        self.url = "http://www.tse.com.tw/exchangeReport/MI_MARGN?response=json&"
        #與網頁不同(需要的資訊要透過計算才能得到)，其中「融資張數, 融券張數」為計算後新增加之欄位名稱
        self.selected_columns = ["日期", '股票代號', '股票名稱', "融資張數", "融券張數"]

    def get_selected_data(self):
        try:
            self.selected_data = self.get_original_data().copy()
            self.selected_data = self.selected_data[self.selected_data.股票代號.apply(lambda x : len(x) == 4)]
            self.selected_data.iloc[:, [5, 6, 11, 12]] = self.selected_data.iloc[:, [5, 6, 11, 12]].applymap(lambda x : float(x.replace(",", "")))
            #計算融資張數
            self.selected_data["融資張數"] = self.selected_data.iloc[:, 6] - self.selected_data.iloc[:, 5] 
            #計算融券張數
            self.selected_data["融券張數"] = self.selected_data.iloc[:, 12] - self.selected_data.iloc[:, 11]
            self.selected_data = self.selected_data[self.selected_columns]
        except:
            self.selected_data = pd.DataFrame([])
        return self.selected_data
#若沒交易日會回傳空值 → 結果會結束在try裡面並回傳有欄位名稱的空dataframe
#若日期未到則會回傳「查詢日期大於今日，請重新查詢!」 → 會進到except


#借券
class Exchange_borrow(Exchange):
    def __init__(self, date, stock_type = "ALL"):
        Exchange.__init__(self, date, stock_type)
        self.url = "http://www.tse.com.tw/exchangeReport/TWT93U?response=json&"
        #與網頁不同(需要的資訊要透過計算才能得到)，其中「借券張數」為計算後新增加之欄位名稱
        self.selected_columns = ['日期', '股票代號', '股票名稱', "借券張數"] 

    def get_selected_data(self):
        try:
            #選擇後半借券的部分
            self.selected_data = self.get_original_data().iloc[:, [15, 0, 1, 8, 12]] 
            self.selected_data = self.selected_data.copy()[self.selected_data.股票代號.apply(lambda x : len(str(x)) == 4)]
            self.selected_data.iloc[:, [3, 4]] = self.selected_data.iloc[:, [3, 4]].applymap(lambda x : float(x.replace(",", "")))
            self.selected_data["借券張數"] = (self.selected_data.iloc[:, 4] - self.selected_data.iloc[:, 3]) / 1000
            self.selected_data = self.selected_data[['日期', '股票代號', '股票名稱', "借券張數"]]
        except:
            self.selected_data = pd.DataFrame([])
        return self.selected_data
#若沒交易日會回傳空值 → 結果會結束在try裡面並回傳有欄位名稱的空dataframe
#若日期未到則會回傳「查詢日期大於今日，請重新查詢!」 → 會進到except


##整理data及計算指標
class deal_with_data:

    def __init__(self):
        #None將透過繼承填入
        self.main_force = None 
        self.margin_purchase_short_sell = None 
        self.borrow = None 
        self.cumulative_data = None
        self.outstanding_stock = None
        self.type = None
        self.market = None
    
    def get_today_data(self):
        #依據日期、股票代號合併
        try:
            self.today_data = self.main_force.merge(self.margin_purchase_short_sell, on = ["日期", "股票代號"], how = "outer")
            self.today_data = self.today_data.merge(self.borrow, on = ["日期", "股票代號"], how = "outer")
            #處理名稱問題，不一定所有股票都會同時存在三類交易，
            #而merge過程中會同時產生不同的股票代號欄位ex.股票代號_x，
            #所以如果挑其中一個則若當支股票沒有在相對應的交易中，則會產生NaN
            name_col = []
            for i in range(len(self.today_data)):
                #依據每一個row將選出unique的值(剔除NaN)
                temp = self.today_data[["股票名稱_x", "股票名稱_y", "股票名稱"]].iloc[i,:].dropna().unique()[0] 
                name_col.append(temp)
            #選出要的欄位
            self.today_data = self.today_data[['日期', '股票代號', '股票名稱', 
                                               '外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '借券張數', 
                                               '融資張數', '融券張數']].copy()
            #把股票名稱指定為處裡過後的名稱
            self.today_data["股票名稱"] = name_col 
            #由於累積資料集在寫入csv檔時會自動變成int，所以這裡也要更改
            self.today_data.股票代號 = self.today_data.股票代號.apply(lambda x : int(x))
            #統一將日期改為str儲存
            self.today_data.日期 = self.today_data.日期.apply(lambda x : str(x)[0:10])
        except:
            self.today_data = pd.DataFrame([])  
        return self.today_data

    def calculate_indicator(self):
        #讀入累積資料
        temp_result = pd.read_csv(self.cumulative_data)
        outstanding_stock = pd.read_csv(self.outstanding_stock)
        #獲取今日資料
        self.today_data = self.get_today_data() 
        print("{} - get today data !".format(self.type))
        #合併回累積資料集
        temp_result = pd.concat([temp_result, self.today_data], axis = 0)
        #去除重複抓取值(以防萬一)
        temp_result.drop_duplicates(inplace = True)
        #寫入(回)累積資料集(迭代)
        temp_result.to_csv(self.cumulative_data, index = False)
        print("{} - data saved !".format(self.type))
        print("{} - calculating indicator...".format(self.type))
        
        #建立空白array等等裝for loop結果
        self.calculation_result = pd.DataFrame([])
        #當中使用到np.arange(-1, -61, -1)都是為了讓速度提升，只用到最後60筆資料即可(但好像沒比較快)
        #建立現有交易日的日期集合(放在for loop外比較快)
        global_time_set = set(temp_result.日期.unique()[np.arange(-1, -61, -1)])
        for company_code in temp_result.股票代號.unique():
            individual_data = temp_result[temp_result.股票代號 == company_code]
            #若資料筆數大過於60則取出最後60筆(資料原先已按照日期排，但取出後順序會顛倒因此要重排)
            if len(individual_data) > 60:
                individual_data = individual_data.iloc[np.arange(-1, -61, -1)].sort_values("日期")
            #建立individual data擁有之交易日期集合
            local_time_set = set(individual_data.日期)
            #若有差集則代表此個股並非所有交易日皆有相關操作資料，因此需要補空值0(由於之後需要用到累加的資料，補植後才不會失真)
            indexs = np.arange(len(global_time_set.difference(local_time_set)))
            temp_output = pd.DataFrame(0, index = indexs, columns = temp_result.columns)
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
            individual_data["當日指標餘額"] = ((individual_data["外陸資買賣超股數(不含外資自營商)"] + 
                                              individual_data["投信買賣超股數"] - 
                                              individual_data["借券張數"]) - 
                                              (individual_data["融資張數"] - 
                                              individual_data["融券張數"]))
            #接著將計算出的指標除上流通在外淨股數
            #流通在外淨股數計算方式為流通在外股數減去董監持股，
            #依據2018/05/15由TEJ抓取的資料，因此並非持續更新的資料，且未扣除庫藏股
            #有些公司可能下市因此首先檢查是否在清單中有此公司
            #(因為可能清單沒有，但爬下來的歷史資料中有此公司)，若下市則直接將此公司資料清空
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
            individual_data = pd.DataFrame(individual_data.sort_values("日期", ascending = True).iloc[-1, :]).T
            self.calculation_result = pd.concat([self.calculation_result, individual_data])
            self.calculation_result[["近5日指標比例", 
                                     "近10日指標比例", 
                                     "近20日指標比例", 
                                     "近60日指標比例"]] = self.calculation_result[["近5日指標比例", 
                                                                                  "近10日指標比例", 
                                                                                  "近20日指標比例", 
                                                                                  "近60日指標比例"]].applymap(lambda x : round(x, 3))
        self.calculation_result = self.calculation_result[["日期", "股票代號", "股票名稱", 
                                                           "外陸資買賣超股數(不含外資自營商)", "投信買賣超股數", "借券張數",
                                                           "融資張數", "融券張數", "當日指標餘額", 
                                                           "近5日指標比例", "近10日指標比例", "近20日指標比例", "近60日指標比例"]]
        #重新指定columns名稱
        self.calculation_result.columns = ["日期", "股票代號", "股票名稱", 
                                           "外陸資買賣超張數", "投信買賣超張數", "借券張數",
                                           "融資張數", "融券張數", "當日指標餘額", 
                                           "近5日指標比例", "近10日指標比例", "近20日指標比例", "近60日指標比例"]
        print("{} - calculation finished !".format(self.type))
        return self.calculation_result
        
    def transform_to_html(self, top_number = 30):
        self.calculation_result = self.calculate_indicator()
        #建立各排序方式(大致小)前30名名單
        sort_by_five = self.calculation_result.sort_values("近5日指標比例", ascending = False).iloc[0:(top_number + 1), :].reset_index(drop = True)
        sort_by_twenty = self.calculation_result.sort_values("近20日指標比例", ascending = False).iloc[0:(top_number + 1), :].reset_index(drop = True)
        sort_by_sixty = self.calculation_result.sort_values("近60日指標比例", ascending = False).iloc[0:(top_number + 1), :].reset_index(drop = True)
        #尋找上述清單中符合20日大於60日指標之股票→命名為selected_1
        all_companies = pd.concat([sort_by_five, sort_by_twenty, sort_by_sixty])
        all_companies.drop_duplicates(inplace = True)
        selected_1_num = sum(all_companies["近20日指標比例"] > all_companies["近60日指標比例"])
        if selected_1_num > 0:
            selected_1 = all_companies[all_companies["近20日指標比例"] > all_companies["近60日指標比例"]]
            selected_1 = selected_1.sort_values("近20日指標比例", ascending = False)
            selected_1 = selected_1.reset_index(drop = True)
            selected_1_set = set(selected_1.股票代號)
        else:
            selected_1 = pd.DataFrame(["NA"], columns = ["無符合條件之個股"])
            selected_1_set = set()
        #尋找上述清單中同時出現在5, 20, 60清單者→命名為selected_2
        selected_2_set = set(sort_by_five.股票代號).intersection(set(sort_by_twenty.股票代號)).intersection(set(sort_by_sixty.股票代號))
        if len(selected_2_set) > 0:
            selected_2 = all_companies[all_companies.股票代號.apply(lambda x : x in selected_2_set)].reset_index(drop = True)
        else:
            selected_2 = pd.DataFrame(["NA"], columns = ["無符合條件之個股"])
        #尋找上述清單中同時出現在5, 20, 60清單且20日大於60日指標之股票→命名為selected_3
        selected_3_set = selected_2_set.intersection(selected_1_set)
        if len(selected_3_set) > 0:
            selected_3 = all_companies[all_companies.股票代號.apply(lambda x : x in selected_3_set)].reset_index(drop = True)
        else:
            selected_3 = pd.DataFrame(["NA"], columns = ["無符合條件之個股"])

        #建立寄信內容
        self.html_result = ""
        self.html_result += "<strong><font size = 4>本日出現於5, 20, 60日清單且20日指標大於60日之個股</font></strong>"
        self.html_result += selected_3.to_html()
        self.html_result += "<br><br>"
        self.html_result += "<strong><font size = 4>本日出現於5, 20, 60日清單之個股</font></strong>"
        self.html_result += selected_2.to_html()
        self.html_result += "<br><br>"
        self.html_result += "<strong><font size = 4>本日清單符合20日指標大於60日之個股</font></strong>"
        self.html_result += selected_1.to_html()
        self.html_result += "<br><br>"
        self.html_result += "<strong><font size = 4>依據近5日指標排序</font></strong>"
        self.html_result += sort_by_five.to_html()
        self.html_result += "<br><br>"
        self.html_result += "<strong><font size = 4>依據近20日指標排序</font></strong>"
        self.html_result += sort_by_twenty.to_html()
        self.html_result += "<br><br>"
        self.html_result += "<strong><font size = 4>依據近60日指標排序</font></strong>"
        self.html_result += sort_by_sixty.to_html()
        return self.html_result

    def final_result(self):
        #檢查當日是否有交易資料，若無則說明今日休市
        self.html_result = ""
        if len(self.main_force) == 0:
            self.html_result += ""
            self.html_result += "<strong><font size = 4>今日休市!</font></strong>"
            return self.html_result
        else:
            return self.transform_to_html()


#兩個重點注意：
#1.這裡想要達到不輸入日期就預設為今日的功能，若上方的parent class裡沒有date此一參數，
#  則可以在繼承時自己加入，接著透過[]搭配if條件式，去完成上述功能
#2.若上方的parent class有date此一參數，仍無法透過在parent class指定date = []接
#  著透過繼承去繼承date = []，仍需要在下方繼承時另外將date = []寫入(無法繼承parent
#  class裡的date = [])，在繼承時寫入date = []將會被首先考量(蓋過parent class中沒有
#  任何指定的date)

#繼承自deal_with_data
class Exchange_deal_with_data(deal_with_data):

    def __init__(self, date = []):
        #init時直接指定date(預設為今日)
        if date == []:
            self.date = int(str(datetime.today())[0:10].replace("-", ""))
        else:
            self.date = date
        self.main_force = Exchange_main_force(self.date).get_selected_data()
        self.margin_purchase_short_sell = Exchange_margin_purchase_short_sell(self.date).get_selected_data()
        self.borrow = Exchange_borrow(self.date).get_selected_data()
        self.cumulative_data = "2018_Exchange_temp_result.csv" 
        self.outstanding_stock = "2018_outstanding_stock.csv"  
        self.type = "Exchange"
        self.market = "上市公司"


#將計算結果以email寄出
class gmail:
    
    def __init__(self, receivers, content):
        self.my_mail = "andy566159@gmail.com"
        self.receivers = receivers
        self.market = content.market
        self.type = content.type
        self.content = content
        self.date = str(content.date)
        self.date = self.date[0:4] + "-" + self.date[4:6] + "-" + self.date[6:8]
        self.smtplib_object = smtplib.SMTP('smtp.gmail.com', 587)
        self.smtplib_object.starttls()
        self.smtplib_object.login(self.my_mail, os.environ["gmail_pwd"])
        
    def send_gmail(self):
        msg = MIMEMultipart('alternative')
        msg["Subject"] = "{} {} 籌碼指標".format(self.market, self.date)
        msg["From"] = self.my_mail
        html = self.content.final_result()
        html_part = MIMEText(html, 'html')
        msg.attach(html_part)
        for receiver in self.receivers:
            msg["to"] = receiver
            self.smtplib_object.sendmail(self.my_mail, receiver, msg.as_string())
        print("{} - email sent !".format(self.type))
        self.smtplib_object.quit()