import pandas as pd
import requests

class Exchange_Crawler:
    def __init__(self, date):
        self.date = str(date)
        self.main_url = None
        self.target = None
        self.select_type='ALLBUT0999'
        
    def get_raw_data(self):
        params = {
            'response': 'json',
            'date': self.date,
            'selectType': self.select_type
        }
        try:
            response = requests.get(self.main_url, params=params, headers = {'Connection':'close'})
            response_dict = response.json()
        except Exception as e:
            print(e)

        # 判斷今日是否有開市，若沒開市則代表沒資料，接著會丟出AssertionError
        # 日後透過for丟入日期更新資料時可以透過except捕捉AssertionError來決定pass，使for繼續運行
        # 但可能還會有其他原因導致'data'不在response_dict中，所以不見得完全是因為沒有開市
        # 此主要對應三大法人買賣超狀況
        assert 'data' in response_dict, 'Market close today !'
        # 此主要對應融資融券、借券的狀況，因為即使休市response中仍然會有data，只是會是空的
        assert len(response_dict['data']) != 0, 'Market close today !'
        self.response_dict = response_dict
        raw_data = self.response_dict['data']

        # 爬下來的值皆為string，因此要轉成float
        def transform_to_float(series):
            # 先定義sub function給內部mapping用
            def sub_transform(x):
                try:
                    # 爬下來的資料單位為股，因此要改成張，但融資融券本身單位即為張所以甭改
                    if self.target == 'Institution' or self.target == 'Borrow':
                        return int(x.replace(',', ''))/1000
                    elif self.target == 'Margin':
                        return int(x.replace(',', ''))
                except:
                    return x

            # 但首兩個columns為股票代號和股票名稱，因此無須轉換
            series[2:] = list(map(
                lambda x: sub_transform(x),
                series[2:]
            ))
            return series

        # 外部mapping，raw_data為一個array包著其他array，而單個array裡有多個值，因此用兩層mapping
        raw_data = list(map(
            lambda x: transform_to_float(x),
            raw_data
        ))

        return raw_data 
    
    def get_formatted_data(self):
        raw_data = self.get_raw_data()
        formatted_data = pd.DataFrame(raw_data, columns=self.response_dict['fields'])
        # 三大法人買賣超的其中一個欄位名稱為「證券代號」，但融資融券、借券皆為「股票代號」
        # 因此以下直接將三大法人買賣超中的「證券代號」改為「股票代號」
        # 之後由於融資融券、借券中的get_formatted_data也會被蓋掉不會繼承，因此無須額外多處理
        formatted_data.rename(columns={'證券代號': '股票代號'}, inplace=True)
        
        # 在首個column插入日期
        formatted_data.insert(0, column='Date', value=int(self.date))
        return formatted_data

# 繼承自Exchange_Crawler
class Exchange_Institution(Exchange_Crawler):
    def __init__(self, date):
        Exchange_Crawler.__init__(self, date)
        self.target = 'Institution'
        self.main_url = 'https://www.twse.com.tw/fund/T86'

# 繼承自Exchange_Crawler，同時改寫內部function
class Exchange_Margin(Exchange_Crawler):
    def __init__(self, date):
        Exchange_Crawler.__init__(self, date)
        self.target = 'Margin'
        self.main_url = 'https://www.twse.com.tw/exchangeReport/MI_MARGN'
        # 融資融券、借券的select_type沒有'ALLBUT0999'，因此更改為'ALL'
        self.select_type = 'ALL'

    def get_formatted_data(self):
        raw_data = self.get_raw_data()
        formatted_data = pd.DataFrame(raw_data)
        # 融資融券與借券的表格有雙重columns名稱，需要特殊處理
        # 否則若只用原先response_dict['fields']會指萃取出單層，會出現columns名稱相同的情況
        # response_dict['groups']含有以下資訊
        # [{'span': 6, 'start': 2, 'title': '融資'},
        #  {'span': 6, 'start': 8, 'title': '融券'}]
        # 可以抓出其中start作為將兩個columns名稱的起點，而兩個start之間隔為6
        first_start_index = self.response_dict['groups'][0]['start']
        second_start_index = self.response_dict['groups'][1]['start']
        first_title = self.response_dict['groups'][0]['title']
        second_title = self.response_dict['groups'][1]['title']
        fix_column_interval = 6
        
        # 重新命名
        self.response_dict['fields'][
            first_start_index: first_start_index+fix_column_interval
        ] = list(map(
            lambda x: first_title+x,
            self.response_dict['fields'][
                first_start_index: first_start_index+fix_column_interval
            ]
        )) 
        self.response_dict['fields'][
            second_start_index: second_start_index+fix_column_interval
        ] = list(map(
            lambda x: second_title+x,
            self.response_dict['fields'][
                second_start_index: second_start_index+fix_column_interval
            ]
        ))
        
        formatted_data.columns = self.response_dict['fields']
        # 在首個column插入日期
        formatted_data.insert(0, column='Date', value=int(self.date))
        return formatted_data

# 繼承自Exchange_Margin
class Exchange_Borrow(Exchange_Margin):
    def __init__(self, date):
        Exchange_Crawler.__init__(self, date)
        self.target = 'Borrow'
        self.main_url = 'https://www.twse.com.tw/exchangeReport/TWT93U'
    
    # 由於借券張數不存在，因此需要自行計算
    def get_formatted_data(self):
        # 呼叫parent class的原始function，接著更改function內容
        formatted_data = super().get_formatted_data()
        formatted_data.insert(
            loc=14,
            column='借券張數',
            value=formatted_data['借券賣出當日餘額'] - formatted_data['借券賣出前日餘額']
        )

        return formatted_data

# Reference
# [Python Inheritance, w3schools](https://www.w3schools.com/python/python_inheritance.asp)
# [Pandas dataframe.insert()](https://www.geeksforgeeks.org/python-pandas-dataframe-insert/)
# [Super() Method Tutorial](https://appdividend.com/2019/01/22/python-super-function-example-super-method-tutorial/)