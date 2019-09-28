from exchange import update_Exchange_data
import pandas as pd
import numpy as np
import time
from datetime import datetime
from progressbar import ProgressBar, Percentage, Bar, Timer, ETA


class Pipeline:
    def __init__(self, date, market='TSE'):
        self.date = date
        # 保留市場選項，之後要新增OTC可以使用
        self.market = market
        self.data = pd.read_csv('Cumulative_data.csv', low_memory=False)
        self.outstanding_stocks = pd.read_csv('Outstanding_stocks.csv')
        self.indicator_collection = pd.read_csv('Exchange_indicator_collection.csv')
        self.trial_time_limit = 5
        # 設立avaliable_date_set
        self.avaliable_date_set = list(self.data['Date'].unique())
        self.avaliable_date_set.sort()
    
    def calculate_indicator(self):
        if self.date not in self.avaliable_date_set:
            print('Getting data...')
            try:
                update_Exchange_data(self.date)
            except:
                trial_time = 1
                while trial_time < self.trial_time_limit:
                    time.sleep(3)
                    print("Extra tried time = {}".format(trial_time))
                    try:
                        trial_time += 1
                        update_Exchange_data(self.date)
                    except AssertionError as e:
                        print(e)
                    except Exception as e:
                        print(e)
            self.__init__(self.date)
        else:
            print('Data is up to date!')

        stock_list = self.outstanding_stocks[self.outstanding_stocks['上市別'] == self.market]
        stock_list = stock_list.copy()
        stock_list['股票代號'] = stock_list['股票代號'].apply(lambda x: str(x))

        try:
            end_index = self.avaliable_date_set.index(self.date)
        except:
            end_index = self.avaliable_date_set[-1]
        target_date_set = set(self.avaliable_date_set[end_index - 59: end_index + 1])

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

            # 若資料筆數少於60則須依據缺少日期補入空值
            if len(current_data) < 60:
                missing_date = set(target_date_set).difference(set(current_data.Date))
                supplementary_data = pd.DataFrame(
                    0, index=range(len(missing_date)), columns=current_data.columns
                )
                supplementary_data['Date'] = missing_date
                current_data = pd.concat([current_data, supplementary_data])

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
    
    def sort_value_by_indicator(self):
        # 建立依據指標欲選取的公司家數
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
        # 讀入既有資料，同時判斷當前輸入日期是否已曾被計算
        # 若已存在則不重新計算，若不存在則重新計算
        target_date = str(self.date)[: 4] + '-' +  str(self.date)[4: 6] + '-' + str(self.date)[6: ]

        if target_date not in self.indicator_collection['日期'].values:
            # 由於indicator_result未曾被計算因此須執行計算
            # 最後還需要經由計算之指標比例排序縮減，才會是最終結果(qualified_companies)
            # 所以不像已被計算過的狀況可以直接指定qualified_companies為讀入的indicator_result
            self.indicator_result = self.calculate_indicator()
            # 若當日指標未曾被計算，則計算完後要更新(存入)既有指標資料
            update_indicator = True
        else:
            self.indicator_result = self.indicator_collection[self.indicator_collection['日期'] == target_date]
            self.qualified_companies = self.indicator_result
            update_indicator = False

        # 若未曾計算指標，則要將指標結果存起來
        if update_indicator:
            # 將符合條件者合併起來
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
            # 主要目的是讓self.sort_value_by_indicator()呼叫始能只對縮減後的self.indicator_result做計算
            self.indicator_result = self.qualified_companies
            # 存起來
            self.indicator_collection = pd.concat([self.indicator_collection, self.indicator_result])
            self.indicator_collection.to_csv('Exchange_indicator_collection.csv', index=False)
        
        def normalize_date(date):
            return str(date)[: 4] + '-' +  str(date)[4: 6] + '-' + str(date)[6: ]
        
        last_second_date = self.avaliable_date_set[-2]
        qualified_code = self.qualified_companies['股票代號'].values
        last_second_date_indicator = self.indicator_collection[
            self.indicator_collection['日期'] == normalize_date(last_second_date)
        ]

        self.qualified_companies = self.qualified_companies.copy()
        self.qualified_companies['最早出現日期'] = np.nan

        for code in qualified_code:
            index = -3
            rolling_indicator_collection = self.indicator_collection[
                self.indicator_collection['日期'] == normalize_date(self.avaliable_date_set[index])
            ]['股票代號']
            
            if code in last_second_date_indicator['股票代號'].values:
                while code in rolling_indicator_collection.values:
                    try:
                        index -= 1
                        rolling_indicator_collection = self.indicator_collection[
                            self.indicator_collection['日期'] == normalize_date(self.avaliable_date_set[index])
                        ]['股票代號']
                    except:
                        break
            else:
                while code not in rolling_indicator_collection.values:
                    try:
                        index -= 1
                        rolling_indicator_collection = self.indicator_collection[
                            self.indicator_collection['日期'] == normalize_date(self.avaliable_date_set[index])
                        ]['股票代號']
                    except:
                        index =None
                        last_show_up_date = None
                        break

            if index != None:
                last_show_up_date = normalize_date(self.avaliable_date_set[index + 1])

            self.qualified_companies.loc[
                self.qualified_companies['股票代號'] == code, '最早出現日期'
            ] = last_show_up_date

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

        last_show_up_date_collection = list(map(
            lambda x: transform_to_date(x),
            self.qualified_companies['最早出現日期']
        ))
        self.last_show_up_date_collection = last_show_up_date_collection

        def minus(pair):
            try:
                return int((pair[0] - pair[1]).days)
            except:
                return 0

        self.qualified_companies['已出現天數'] = list(map(
            lambda x: minus(x),
            zip(current_today_collection, last_show_up_date_collection)
        ))

        # 此時qualified_companies才帶有'已出現天數'的資訊
        # 以下要重新執行self.sort_value_by_indicator()，以確保各group皆帶有'已出現天數'資訊
        # 但self.sort_value_by_indicator()是透過self.indicator_result計算
        # 因此要把最新的self.qualified_companies指定回self.indicator_result，才能執行
        self.indicator_result = self.qualified_companies
        self.sort_value_by_indicator()


    def transform_result_to_html(self):
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

                group = group.applymap(lambda x: transform_to_int(x))
            except:
                pass
        
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