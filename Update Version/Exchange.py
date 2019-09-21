import pandas as pd
import requests

class Exchange_Crawler:
    def __init__(self, date):
        self.date = str(date)
        self.main_url = None
        
    def get_raw_data(self, select_type='ALLBUT0999'):
        params = {
            'response': 'json',
            'date': self.date,
            'selectType': select_type
        }
        try:
            response = requests.get(self.main_url, params=params, headers = {'Connection':'close'})
            response_dict = response.json()
        except Exception as e:
            print(e)

        # 判斷今日是否有開市，若沒開市則代表沒資料，接著會丟出AssertionError
        # 日後透過for丟入日期更新資料時可以透過except捕捉AssertionError來決定pass，使for繼續運行
        # 但可能還會有其他原因導致'data'不在response_dict中，所以不見得完全是因為沒有開市
        assert 'data' in response_dict, 'Market close today !'
        self.response_dict = response_dict
        raw_data = self.response_dict['data']
        return raw_data 
    
    def get_formatted_data(self):
        raw_data = self.get_raw_data()
        formatted_data = pd.DataFrame(raw_data, columns=self.response_dict['fields'])
        
        # 在首個column插入日期
        formatted_data.insert(0, column='Date', value=self.date)
        return formatted_data

# 繼承自Exchange_Crawler
class Exchange_Institution(Exchange_Crawler):
    def __init__(self, date):
        Exchange_Crawler.__init__(self, date)
        self.main_url = 'https://www.twse.com.tw/fund/T86'

# 繼承自Exchange_Crawler，同時改寫內部function
class Exchange_Margin(Exchange_Crawler):
    def __init__(self, date):
        Exchange_Crawler.__init__(self, date)
        self.main_url = 'https://www.twse.com.tw/exchangeReport/MI_MARGN'

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
        fix_column_number = 6
        
        # 重新命名
        self.response_dict['fields'][
            first_start_index: first_start_index+fix_column_number
        ] = list(map(
            lambda x: first_title+x,
            self.response_dict['fields'][
                first_start_index: first_start_index+fix_column_number
            ]
        )) 
        self.response_dict['fields'][
            second_start_index: second_start_index+fix_column_number
        ] = list(map(
            lambda x: second_title+x,
            self.response_dict['fields'][
                second_start_index: second_start_index+fix_column_number
            ]
        ))
        
        formatted_data.columns = self.response_dict['fields']
        # 在首個column插入日期
        formatted_data.insert(0, column='Date', value=self.date)
        return formatted_data

# 繼承自Exchange_Margin
class Exchange_Borrow(Exchange_Margin):
    def __init__(self, date):
        Exchange_Crawler.__init__(self, date)
        self.main_url = 'https://www.twse.com.tw/exchangeReport/TWT93U'

# Reference
# [Python Inheritance, w3schools](https://www.w3schools.com/python/python_inheritance.asp)
# [Pandas dataframe.insert()](https://www.geeksforgeeks.org/python-pandas-dataframe-insert/)