import pandas as pd
import requests
import time

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
        try:
            formatted_data.insert(
                8,
                column='融資張數',
                value=formatted_data['融資今日餘額'] - formatted_data['融資前日餘額']
            )

            formatted_data.insert(
                15,
                column='融券張數',
                value=formatted_data['融券今日餘額'] - formatted_data['融券前日餘額']
            )
        # 由於會被Exchange_Borrow直接繼承
        # 但Exchange_Borrow欲爬取的頁面不會有融資的資訊所以會產生error因此會進到except
        # 網頁架構不變下，會進到error的情況基本上只會當Exchange_Borrow繼承併執行get_formatted_data時
        # 但這寫法似乎不是好，物件間的差異用try except去區分感覺不太好
        # 應該要產一個新的物件讓他們去繼承，然後再調細節差異給不同屬性或function
        except Exception as e:
            pass

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

# 將上面class包起來，同時寫進csv中
def update_Exchange_data(date, sleep_time=2):
    cumulative_data = pd.read_csv('Cumulative_data.csv', low_memory=False)
    # 先清一次重複值的資料
    cumulative_data.drop_duplicates(inplace=True)

    assert sleep_time >= 2, 'sleep time太短可能會被證交所封鎖！'
    # 爬取資料
    Institution = Exchange_Institution(date).get_formatted_data()

    time.sleep(sleep_time)
    Margin = Exchange_Margin(date).get_formatted_data()

    time.sleep(sleep_time)
    Borrow = Exchange_Borrow(date).get_formatted_data()

    # 合併資料
    newest_data = Institution.merge(
        Margin, on=['Date', '股票代號'], how='outer'
    )

    # 由於借券中也涵蓋融資融券資料，如此一來會有重複columns，因此以下只取出純借券部分
    difference_of_columns = Borrow.columns.difference(Margin.columns)
    columns_to_use = ['Date', '股票代號']
    columns_to_use.extend(difference_of_columns)

    newest_data = newest_data.merge(
        Borrow[columns_to_use], on=['Date', '股票代號'], how='outer'
    )
    
    # 將float都四捨五入到小數點，否則後續在drop_duplicates時可能會有看似相近，但其實不同的float
    def transform_to_int(x):
        try:
            return round(x)
        except:
            return x
    newest_data = newest_data.applymap(lambda x: transform_to_int(x))
    
    # 先把當日整理後的資料存成csv再重新讀入，確保格式與既有之data相同
    # 主要是nan所導致的問題，原先資料中的nan原本為''(空的string)，但被存進csv後再重新讀入會變成NaN
    # 而當下爬下來的值為''時，若透過if判斷並將其轉成np.nan(或math.nan)，仍然會與既有資料的nan不同
    # 這樣在執行drop_duplicates時會有問題，雖然都是nan但本質上不同，所以重複值出現時不會被drop掉
    # 不確定確切原因為何，reference中有篇文章可以參考
    # 目前解法是將資料先存成csv再重新讀入，如此一來nan值就會相同，因此執行drop_duplicates時便沒有問題
    # 看起來多此一舉，但某種程度而言這個多出來的檔案以可以被拿來當成驗證資料與網頁是否相同的管道
    newest_data.to_csv('Newest_data.csv', index=False)

    # 重新讀入newest_data
    newest_data = pd.read_csv('Newest_data.csv')
    # 合併回累積資料並再做一次drop_duplicates
    cumulative_data = pd.concat([cumulative_data, newest_data])
    cumulative_data.drop_duplicates(inplace=True)
    
    # 依據Date排序，方便未來開csv檔檢查
    cumulative_data.sort_values('Date', inplace=True)

    # 最後再存回Cumulative_data
    cumulative_data.to_csv('Cumulative_data.csv', index=False)

    print('Data updated!')




# Reference
# [Python Inheritance, w3schools](https://www.w3schools.com/python/python_inheritance.asp)
# [Pandas dataframe.insert()](https://www.geeksforgeeks.org/python-pandas-dataframe-insert/)
# [Super() Method Tutorial](https://appdividend.com/2019/01/22/python-super-function-example-super-method-tutorial/)
# [Safest way to convert float to integer in python?](https://stackoverflow.com/questions/3387655/safest-way-to-convert-float-to-integer-in-python)
# [Why is float('nan') not equal to itself in python [duplicate]](https://stackoverflow.com/questions/45022451/why-is-floatnan-not-equal-to-itself-in-python)