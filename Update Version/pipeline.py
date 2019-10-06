from exchange import update_Exchange_data
import pandas as pd
import numpy as np
import time
from datetime import datetime
from progressbar import ProgressBar, Percentage, Bar, Timer, ETA
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class Pipeline:
    def __init__(self, date=[], market='TSE'):
        # 若未指定日期則預設為今日
        if date == []:
            self.date = int(str(datetime.today())[0:10].replace("-", ""))
        else:
            self.date = date
        # 保留市場選項，之後要新增OTC可以使用
        self.market = market
        # 累積的籌碼資料
        self.data = pd.read_csv('Cumulative_data.csv', low_memory=False)
        # 流通在外股數
        self.outstanding_stocks = pd.read_csv('Outstanding_stocks.csv')
        # 累積籌碼指標
        self.indicator_collection = pd.read_csv('Exchange_indicator_collection.csv')
        # 重新丟request的上限次數
        self.trial_time_limit = 5
        # 目前儲存籌碼資料中的日期範圍
        self.avaliable_date_set = list(self.data['Date'].unique())
        self.avaliable_date_set.sort()
    
    # 計算指標會花較長時間，目前沒有進行優化，例如使用np.array方式改寫
    # 獨立成一個function的目的在於執行前會先判斷既有籌碼指標中是否已曾計算
    # 若已計算則不必重複計算
    def calculate_indicator(self):
        # 會先判斷輸入日期日否已存在累積籌碼資料中，若否則執行爬蟲更新資料
        # 由於沒有抓另外一類資料是「今日是否開盤」，因此無法判斷今日到底有無交易資料
        # 因此透過trial方式當出現error時重新執行爬蟲值到達到上限次數
        # 若有error在update_Exchange_data也有try, except機制會同步顯示Exception
        if self.date not in self.avaliable_date_set:
            print('Getting data...')
            try:
                update_Exchange_data(self.date)
            except:
                trial_time = 1
                while trial_time <= self.trial_time_limit:
                    time.sleep(3)
                    print("Extra trial = {}".format(trial_time))
                    # 其實這裡設計不好，若try過程中成功，程式還是會繼續丟request
                    # 直到trial_time_limit達到上限才會停止
                    # 可以設計機制判斷有資料新增就break
                    try:
                        trial_time += 1
                        update_Exchange_data(self.date)
                    except AssertionError as e:
                        print(e)
                    except Exception as e:
                        print(e)
            # 若資料更新成功則重新init，因為要將資料重新讀入物件
            self.__init__(self.date)
        else:
            print('Data is up to date!')

        stock_list = self.outstanding_stocks[self.outstanding_stocks['上市別'] == self.market]
        stock_list = stock_list.copy()
        stock_list['股票代號'] = stock_list['股票代號'].apply(lambda x: str(x))

        # 找當前給定日期對應到既有日期序列中的位置，並往回找60筆日期，將用於抓區間籌碼資料
        try:
            end_index = self.avaliable_date_set.index(self.date)
        except:
            # 若遇到日期不在既有區間中則給予最新日期，可能情形有過去沒交易的日期或是未到來的交易日
            end_index = len(self.avaliable_date_set) - 1
        target_date_set = set(self.avaliable_date_set[end_index - 59: end_index + 1])

        print('Calculating indicator...')
        indicator_result = pd.DataFrame()

        # 使用progressbar顯示進度，以下設定相關outlay
        widgets = [
            'Progress: ', Percentage(), ' ',
            Bar('#'),' ', Timer(),
            ' , ', ETA(), ' '
        ]
        bar = ProgressBar(widgets=widgets, maxval=len(stock_list)).start()
        # inside for loop
        for i in range(len(stock_list)):
            current_code = stock_list.iloc[i]['股票代號']
            current_company_name = stock_list.iloc[i]['股票名稱']

            current_data = self.data[self.data['股票代號'] == current_code]
            current_data = current_data[current_data.Date.isin(target_date_set)]
            current_outstanding_stocks = self.outstanding_stocks[
                self.outstanding_stocks['股票代號'] == int(current_code)
            ]['淨流通在外張數'].values[0]

            assert len(current_data) <= 61, '資料重覆，請檢查，否則指標將失真！'

            # 若資料筆數少於60則須依據缺少日期補入空值，否則計算出的指標會有問題
            if len(current_data) < 60:
                missing_date = set(target_date_set).difference(set(current_data.Date))
                supplementary_data = pd.DataFrame(
                    0, index=range(len(missing_date)), columns=current_data.columns
                )
                supplementary_data['Date'] = missing_date
                current_data = pd.concat([current_data, supplementary_data])
            
            # na值意謂沒有實際交易數字因此給0
            current_data.fillna(0, inplace=True)
            current_data.sort_values('Date', inplace=True)
            
            # 計算指標
            current_data["當日指標餘額"] = (
                current_data['外陸資買賣超股數(不含外資自營商)'] +
                current_data['投信買賣超股數'] -
                current_data['借券張數'] - 
                current_data['融資張數'] + 
                current_data['融券張數']
            )

            for num in [5, 10, 20, 60]:
                column_name = '近{}日指標比例'.format(num)
                current_data[column_name] = list(map(
                    lambda x: round(x, 2),
                    current_data['當日指標餘額'].rolling(num).sum() / current_outstanding_stocks * 100
                ))

            temp_result = current_data[[
                'Date', '股票代號',
                '外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '借券張數', '融資張數', '融券張數',
                '當日指標餘額', '近5日指標比例', '近10日指標比例', '近20日指標比例', '近60日指標比例'
            ]]

            temp_result = pd.DataFrame(temp_result.iloc[-1, ]).T
            # 怕遇到最後一期沒資料者，此時代號會是0，因此直接指定以避免此狀況
            temp_result['股票代號'] = current_code
            temp_result.insert(2, '股票名稱', current_company_name)
            indicator_result = pd.concat([indicator_result, temp_result])
            bar.update(i)
        bar.finish()
        print('Calculation finished!')

        def transform_to_date(x):
            try:
                return datetime.strftime(
                    datetime.strptime(str(x), '%Y%m%d'),
                    '%Y-%m-%d'
                )
            except:
                return x
        indicator_result['Date'] = indicator_result['Date'].apply(
            lambda x: transform_to_date(x)
        )
        indicator_result = indicator_result.rename(columns={
            'Date': '日期',
            '外陸資買賣超股數(不含外資自營商)': '外陸資買賣超張數',
            '投信買賣超股數': '投信買賣超張數'
        })
        self.indicator_result = indicator_result
        return self.indicator_result
    
    # 此self.function會重複被使用所以獨立出來
    def sort_value_by_indicator(self):
        # 設定將依據指標選取的公司家數
        top_number = 30

        # 建立各排序方式(大致小)前30名名單
        self.sort_by_five = self.indicator_result.sort_values("近5日指標比例", ascending=False).iloc[
            :top_number, 
        ]
        self.sort_by_five.index = range(1, top_number+1)

        self.sort_by_twenty = self.indicator_result.sort_values("近20日指標比例", ascending = False).iloc[
            :top_number, 
        ]
        self.sort_by_twenty.index = range(1, top_number+1)

        self.sort_by_sixty = self.indicator_result.sort_values("近60日指標比例", ascending = False).iloc[
            :top_number, 
        ]
        self.sort_by_sixty.index = range(1, top_number+1)

    def select_qualified_companies(self):
        target_date = str(self.date)[: 4] + '-' +  str(self.date)[4: 6] + '-' + str(self.date)[6: ]

        # 判斷當前輸入日期是否已曾被計算，若已存在則不重新計算，若不存在則重新計算
        if target_date not in self.indicator_collection['日期'].values:
            # 由於indicator_result未曾被計算因此須執行計算
            # 最後還需要經由計算之指標比例排序縮減，才會是最終結果(qualified_companies)
            # 所以不像已被計算過的狀況可以直接指定qualified_companies為讀入的indicator_result
            self.indicator_result = self.calculate_indicator()
            # 若當日指標未曾被計算，則計算完後要更新(存入)既有指標資料
            update_indicator = True
        else:
            print('Data is up to date!')
            print('Calculating indicator...')
            self.indicator_result = self.indicator_collection[self.indicator_collection['日期'] == target_date]
            self.qualified_companies = self.indicator_result
            update_indicator = False

        # 若未曾計算指標，則要將指標結果存起來
        if update_indicator:
            # 若指標未曾被計算則要透過執行sort_value_by_indicator()才能產生各類別排序結果
            # 因為最後要存的只有各類別排序結果，並沒有要存所有公司的指標
            self.sort_value_by_indicator()
            qualified_companies = pd.concat([self.sort_by_five, self.sort_by_twenty, self.sort_by_sixty])
            qualified_companies.drop_duplicates(['日期', '股票代號'], inplace=True)
            self.qualified_companies = qualified_companies
            # 股票代號原先是string要轉成int，否則下面在找首次出現日期時會跳錯誤
            # 因為indicator_collexction裡面的股票代號會是int
            self.qualified_companies['股票代號'] = self.qualified_companies['股票代號'].apply(
                lambda x: int(x)
            )
            # 將indicator縮減至經篩選過後的qualified_companies，使此兩變數相同
            # 主要目的是讓執行self.sort_value_by_indicator()時，只對縮減後的self.indicator_result做計算
            self.indicator_result = self.qualified_companies
            # 存起來
            self.indicator_collection = pd.concat([self.indicator_collection, self.indicator_result])
            # 若是修市的日期因為不在既有資料中，所以會進到update_indicator中
            # 同時又會計算一次最新日期的指標，會被寫入indicator_collection
            # 如此一來會重覆，所以要drop_duplicates
            self.indicator_collection.drop_duplicates(['日期', '股票代號'], inplace=True)
            self.indicator_collection.to_csv('Exchange_indicator_collection.csv', index=False)

        # 以下找尋連續出現情形下最早出現的日期，與不連續出現情形下最後出現的日期
        # 既有資料中倒數第二筆資料日期，但須考量給定日期，所以透過index去找位置
        try:
            last_second_date_index = self.avaliable_date_set.index(self.date)-1
        except:
            # 若遇到日期不在既有區間中則給予最新日期，可能情形有過去沒交易的日期或是未到來的交易日
            last_second_date_index = len(self.avaliable_date_set) - 1
        last_second_date = self.avaliable_date_set[last_second_date_index]
        # 欲進行判斷的標的
        qualified_code = self.qualified_companies['股票代號'].values

        def normalize_date(date):
            return str(date)[: 4] + '-' +  str(date)[4: 6] + '-' + str(date)[6: ]

        # 找尋倒數第二個交易日之指標資料目的在於協助以下連續出現日的判斷
        last_second_date_indicator = self.indicator_collection[
            self.indicator_collection['日期'] == normalize_date(last_second_date)
        ]

        self.qualified_companies = self.qualified_companies.copy()
        self.qualified_companies['連續出現下最早出現日期'] = np.nan
        self.qualified_companies['已出現天數'] = np.nan
        self.qualified_companies['不連續出現下最後出現日期'] = np.nan

        for code in qualified_code:
            # 從給定日期下，倒數第二個交易日開始找，index將會一直被-=1
            initial_index = -2
            # 透過上述倒數第二個交易日的下一天來判斷最後一個交易日
            last_index = last_second_date_index + 1
            rolling_indicator_collection = self.indicator_collection[
                self.indicator_collection['日期'] == normalize_date(
                    self.avaliable_date_set[last_index + initial_index]
                )
            ]['股票代號']
            
            find_earliest = False
            find_last = False

            # 股票在最新一筆資料一定有上榜(各類指標前30名)存在才會被考量
            # 因此接下來有兩個方向需要判斷
            # 1. 連續出現下最早出現日期與已出現天數
            # 若股票在給定日期的上一日有上榜，則代表需要找連續往回推的時間中何時未出現在榜上
            # 便可以判斷此未出現日期的下一個交易日便是此種情況下的最早出現日期，同時需要計算已出現天數
            # 2. 不連續出現下最後出現日期
            # 若股票在給定日期的上一日沒有上榜，則代表連續往回推中首次出現的日期便是最後出現日期
            if code in last_second_date_indicator['股票代號'].values:
                while code in rolling_indicator_collection.values:
                    find_earliest = True
                    try:
                        initial_index -= 1
                        rolling_indicator_collection = self.indicator_collection[
                            self.indicator_collection['日期'] == normalize_date(
                                self.avaliable_date_set[last_index + initial_index]
                            )
                        ]['股票代號']
                    except:
                        break
            else:
                while code not in rolling_indicator_collection.values:
                    find_last = True
                    try:
                        initial_index -= 1
                        rolling_indicator_collection = self.indicator_collection[
                            self.indicator_collection['日期'] == normalize_date(
                                self.avaliable_date_set[last_index + initial_index]
                            )
                        ]['股票代號']
                    except:
                        initial_index = None
                        target_show_up_date = None
                        break

            if initial_index != None:
                target_show_up_date = normalize_date(
                    self.avaliable_date_set[last_index + initial_index + 1]
                )

            if find_earliest:
                self.qualified_companies.loc[
                    self.qualified_companies['股票代號'] == code, '連續出現下最早出現日期'
                ] = target_show_up_date
            
            if find_last:
                self.qualified_companies.loc[
                    self.qualified_companies['股票代號'] == code, '不連續出現下最後出現日期'
                ] = target_show_up_date

        def transform_to_date(x):
            try:
                return datetime.strptime(x, '%Y-%m-%d')
            except:
                return None
        
        current_today_collection = list(map(
            lambda x: transform_to_date(x),
            self.qualified_companies['日期']
        ))
        self.current_today_collection = current_today_collection
        
        # 計算連續出現下最早出現日期之已出現天數
        earliest_show_up_date_collection = list(map(
            lambda x: transform_to_date(x),
            self.qualified_companies['連續出現下最早出現日期']
        ))
        self.earliest_show_up_date_collection = earliest_show_up_date_collection
        def minus(pair):
            try:
                return int((pair[0] - pair[1]).days)
            except:
                return 0

        self.qualified_companies['已出現天數'] = list(map(
            lambda x: minus(x),
            zip(current_today_collection, earliest_show_up_date_collection)
        ))

        # 此時qualified_companies才帶有上述新增的資訊
        # 以下要重新執行self.sort_value_by_indicator()，以確保各group皆帶有上述新增的資訊
        # 但self.sort_value_by_indicator()是透過self.indicator_result計算
        # 因此要把最新的self.qualified_companies指定回self.indicator_result
        # 重新產生各指標類別排序，因為面寄信部分需要各類別資訊
        self.indicator_result = self.qualified_companies
        self.sort_value_by_indicator()


    def transform_result_to_html(self):
        # 執行上述多重步驟
        # self.calculate_indicator(), self.sort_value_by_indicator都被包在其中
        self.select_qualified_companies()

        # 尋找上述清單中符合20日大於60日指標之股票→命名為group_1
        group_1_num = sum(self.qualified_companies["近20日指標比例"] > self.qualified_companies["近60日指標比例"])
        if group_1_num > 0:
            group_1 = self.qualified_companies[
                self.qualified_companies["近20日指標比例"] > self.qualified_companies["近60日指標比例"]
            ]
            group_1 = group_1.sort_values("近20日指標比例", ascending = False)
            group_1.index = range(1, group_1_num+1)
            group_1_set = set(group_1['股票代號'])
        else:
            group_1 = pd.DataFrame(["NA"], columns=["無符合條件之個股"])
            group_1_set = set()
            
        # 尋找上述清單中同時出現在5, 20, 60清單者→命名為group_2
        group_2_set = set(self.sort_by_five.股票代號).intersection(
            set(self.sort_by_twenty['股票代號'])
        ).intersection(
            set(self.sort_by_sixty['股票代號'])
        )
        group_2_num = len(group_2_set)
        if group_2_num > 0:
            group_2 = self.qualified_companies[self.qualified_companies['股票代號'].isin(group_2_set)]
            group_2 = group_2.sort_values('近5日指標比例', ascending = False)
            group_2.index = range(1, group_2_num+1)
        else:
            group_2 = pd.DataFrame(["NA"], columns=["無符合條件之個股"])
            
        # 尋找上述清單中同時出現在5, 20, 60清單且20日大於60日指標之股票→命名為group_3
        group_3_set = group_2_set.intersection(group_1_set)
        group_3_num = len(group_3_set)
        if len(group_3_set) > 0:
            group_3 = self.qualified_companies[self.qualified_companies['股票代號'].isin(group_3_set)]
            group_3 = group_3.sort_values('近5日指標比例', ascending = False)
            group_3.index = range(1, group_3_num+1)
        else:
            group_3 = pd.DataFrame(["NA"], columns=["無符合條件之個股"])

        def transform_to_int(x):
            try:
                return int(x)
            except:
                return x

        def transform_to_percentage(x):
            return str(x) + '%'

        for group in [group_1, group_2, group_3]:
            # 可能會出現無符合條件個股的情況，所以用try
            try:
                group[[
                    '近5日指標比例', '近10日指標比例', '近20日指標比例', '近60日指標比例'
                ]] = group[[
                    '近5日指標比例', '近10日指標比例', '近20日指標比例', '近60日指標比例'
                ]].applymap(lambda x: transform_to_percentage(x))

                group[group.columns] = group[group.columns].applymap(lambda x: transform_to_int(x))
            except:
                pass
        
        for sorted_result in [self.sort_by_five, self.sort_by_twenty, self.sort_by_sixty]:
            try:
                sorted_result[[
                    '近5日指標比例', '近10日指標比例', '近20日指標比例', '近60日指標比例'
                ]] = sorted_result[[
                    '近5日指標比例', '近10日指標比例', '近20日指標比例', '近60日指標比例'
                ]].applymap(lambda x: transform_to_percentage(x))

                sorted_result[sorted_result.columns] = sorted_result[sorted_result.columns].applymap(
                    lambda x: transform_to_int(x)
                )
            except:
                pass

            sorted_result[sorted_result.columns] = sorted_result[sorted_result.columns].applymap(
                lambda x: transform_to_int(x)
            )

        html_result = ""
        html_result += "<strong><font size = 4>本日出現於5, 20, 60日清單且20日指標大於60日之個股</font></strong>"
        html_result += group_3.to_html()
        html_result += "<br><br>"
        html_result += "<strong><font size = 4>本日出現於5, 20, 60日清單之個股</font></strong>"
        html_result += group_2.to_html()
        html_result += "<br><br>"
        html_result += "<strong><font size = 4>本日清單符合20日指標大於60日之個股</font></strong>"
        html_result += group_1.to_html()
        html_result += "<br><br>"
        html_result += "<strong><font size = 4>依據近5日指標排序</font></strong>"
        html_result += self.sort_by_five.to_html()
        html_result += "<br><br>"
        html_result += "<strong><font size = 4>依據近20日指標排序</font></strong>"
        html_result += self.sort_by_twenty.to_html()
        html_result += "<br><br>"
        html_result += "<strong><font size = 4>依據近60日指標排序</font></strong>"
        html_result += self.sort_by_sixty.to_html()
        self.html_result = html_result
        return self.html_result
    
class Gmail:
    def __init__(self, receivers_list, pipeline):
        self.my_mail = "andy566159@gmail.com"
        self.receivers_list = receivers_list
        self.pipeline = pipeline
        self.market = pipeline.market
        # 預留之後如果要使用OTC的選項
        if self.market == 'TSE':
            self.market_name = '上市公司'
        elif self.market == 'OTC':
            self.market_name = '上櫃公司'
        date = str(self.pipeline.date)
        self.date = date[0:4] + "-" + date[4:6] + "-" + date[6:8]
        self.smtplib_object = smtplib.SMTP('smtp.gmail.com', 587)
        self.smtplib_object.starttls()
        self.smtplib_object.login(self.my_mail, os.environ["gmail_pwd"])
        
    def send_gmail(self):
        msg = MIMEMultipart('alternative')
        html_result = self.pipeline.transform_result_to_html()
        if self.pipeline.date not in self.pipeline.avaliable_date_set:
            if datetime.now().hour > 22:
                msg["Subject"] = "{} {} 籌碼指標 {}".format(
                    self.market_name,
                    self.date,
                    "(當日休市，以下為最近一期資料)"
                )
            else:
                msg["Subject"] = "{} {} 籌碼指標 {}".format(
                    self.market_name,
                    self.date,
                    "(網站尚未更新資料，以下為最近一期資料)"
                )
        else:
            if self.pipeline.date == int(str(datetime.today())[0:10].replace("-", "")):
                msg["Subject"] = "{} {} 籌碼指標".format(
                    self.market_name,
                    self.date
                )
            else:
                msg["Subject"] = "{} {} 籌碼指標 {}".format(
                    self.market_name,
                    self.date,
                    "(非最新資料)"
                )

        msg["From"] = self.my_mail
        html_part = MIMEText(html_result, 'html')
        msg.attach(html_part)
        for receiver in self.receivers_list:
            msg["to"] = receiver
            self.smtplib_object.sendmail(self.my_mail, receiver, msg.as_string())
        print("{} - email sent !".format(self.market))
        self.smtplib_object.quit()