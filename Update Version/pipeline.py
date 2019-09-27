from exchange import update_Exchange_data
import pandas as pd
from datetime import datetime
from progressbar import ProgressBar, Percentage, Bar, Timer, ETA


class Pipeline:
    def __init__(self, date, market='TSE'):
        self.date = date
        # 保留市場選項，之後要新增OTC可以使用
        self.market = market
        self.data = pd.read_csv('Cumulative_data.csv', low_memory=False)
        self.outstanding_stocks = pd.read_csv('Outstanding_stocks.csv')
    
    def calculate_indicator(self):
        stock_list = self.outstanding_stocks[self.outstanding_stocks['上市別'] == self.market]
        stock_list = stock_list.copy()
        stock_list['股票代號'] = stock_list['股票代號'].apply(lambda x: str(x))

        avaliable_date_set = list(self.data['Date'].unique())
        avaliable_date_set.sort()
        end_index = avaliable_date_set.index(self.date)
        target_date_set = set(avaliable_date_set[end_index - 59: end_index + 1])

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
        indicator_result = indicator_result.rename(columns={
            '外陸資買賣超股數(不含外資自營商)': '外陸資買賣超張數',
            '投信買賣超股數': '投信買賣超張數'
        })

        self.indicator_result = indicator_result
        return self.indicator_result

    def transform_result_to_html(self):
        # 建立依據指標欲選取的公司家數
        top_number = 30

        # 讀入既有資料，同時判斷當前輸入日期是否已曾被計算
        # 若已存在則不重新計算，若不存在則重新計算
        indicator_collection = pd.read_csv('Exchange_indicator_collection.csv')
        target_date = str(self.date)[: 4] + '-' +  str(self.date)[4: 6] + '-' + str(self.date)[6: ]

        if target_date not in indicator_collection['日期'].values:
            indicator_result = self.calculate_indicator()
        else:
            indicator_result = indicator_collection[indicator_collection['日期'] == target_date]

        #建立各排序方式(大致小)前30名名單
        sort_by_five = indicator_result.sort_values("近5日指標比例", ascending=False).iloc[
            :top_number, 
        ]
        sort_by_five.index = range(1, top_number+1)

        sort_by_twenty = indicator_result.sort_values("近20日指標比例", ascending = False).iloc[
            :top_number, 
        ]
        sort_by_twenty.index = range(1, top_number+1)

        sort_by_sixty = indicator_result.sort_values("近60日指標比例", ascending = False).iloc[
            :top_number, 
        ]
        sort_by_sixty.index = range(1, top_number+1)

        # 將符合條件者合併起來
        qualified_companies = pd.concat([sort_by_five, sort_by_twenty, sort_by_sixty])
        self.qualified_companies = qualified_companies
        qualified_companies.drop_duplicates(['Date', '股票代號'], inplace=True)

        qualified_companies['Date'] = qualified_companies['Date'].apply(
            lambda x: datetime.strptime(str(x), '%Y%m%d')
        )
        qualified_companies.rename(columns={'Date': '日期'}, inplace=True)

        #尋找上述清單中符合20日大於60日指標之股票→命名為group_1
        group_1_num = sum(qualified_companies["近20日指標比例"] > qualified_companies["近60日指標比例"])
        if group_1_num > 0:
            group_1 = qualified_companies[
                qualified_companies["近20日指標比例"] > qualified_companies["近60日指標比例"]
            ]
            group_1 = group_1.sort_values("近20日指標比例", ascending = False)
            group_1.index = range(1, group_1_num+1)
            group_1_set = set(group_1['股票代號'])
        else:
            group_1 = pd.DataFrame(["NA"], columns=["無符合條件之個股"])
            group_1_set = set()
            
        #尋找上述清單中同時出現在5, 20, 60清單者→命名為group_2
        group_2_set = set(sort_by_five.股票代號).intersection(
            set(sort_by_twenty['股票代號'])
        ).intersection(
            set(sort_by_sixty['股票代號'])
        )
        group_2_num = len(group_2_set)
        if group_2_num > 0:
            group_2 = qualified_companies[qualified_companies['股票代號'].isin(group_2_set)]
            group_2 = group_2.sort_values('近5日指標比例', ascending = False)
            group_2.index = range(1, group_2_num+1)
        else:
            group_2 = pd.DataFrame(["NA"], columns=["無符合條件之個股"])
            
        #尋找上述清單中同時出現在5, 20, 60清單且20日大於60日指標之股票→命名為group_3
        group_3_set = group_2_set.intersection(group_1_set)
        group_3_num = len(group_3_set)
        if len(group_3_set) > 0:
            group_3 = qualified_companies[qualified_companies['股票代號'].isin(group_3_set)]
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
        html_result += sort_by_five.to_html()
        html_result += "<br><br>"
        html_result += "<strong><font size = 4>依據近20日指標排序</font></strong>"
        html_result += sort_by_twenty.to_html()
        html_result += "<br><br>"
        html_result += "<strong><font size = 4>依據近60日指標排序</font></strong>"
        html_result += sort_by_sixty.to_html()
        self.html_result = html_result
        return self.html_result