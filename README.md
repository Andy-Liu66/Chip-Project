## 爬取上市櫃公司籌碼資料，計算自定義指標並以email寄送結果

### 簡介
透過Requests套件自**台灣證券交易所**及**櫃檯買賣中心**爬取資料，存入csv檔(目前未使用資料庫)，以迭代的方式持續更新資料，在資料儲存完成後計算自定義籌碼指標，並將結果表格轉為HTML表格，以email方式寄送，自動化部分則使用Windows Task Scheduler，每日執行.py檔。

### 欲爬取資料
自「台灣證券交易」及「櫃檯買賣中心」分別爬取「三大法人買賣超」、「融資融券」、「借券」等資料，因此須從6(2*3)個網頁來源爬取資料。

### 使用套件
1. pandas
2. numpy
3. datetime
4. requests
5. json
6. os
7. smtplib
8. email.mime.text.MIMEText
9. email.mime.multipart.MIMEMultipart

### 功能說明
爬取台灣證券交易所資料之程式放置於**Exchange**模組中，櫃檯買賣中心之程式則放置於**OTC**模組中。各模組包含「*main_force, margin_purchase_short_sell, borrow*」等物件(需在前面加上*Exchange_*或是*OTC_*)，分別處理「三大法人買賣超、融資融券、借券」，也包含「*deal_with_data*」此物件，將上述三者的功能合併，並處理存入csv檔、計算自定義籌碼指標、將計算結果轉為HTML表格等功能，以下將以上市公司為例(Exchange)分別介紹功能：

* **三大法人買賣超**
```python
#載入Exchange
from Exchange import *

#建立main_force物件，填入欲爬取日期及類別(類別若不填入則預設為"ALL"，下方提供類別列表)
#類別參數只有在爬取上市公司時可選擇，至於上櫃公司資料則無法選擇(沒有類別此一參數)
main_force = Exchange_main_force(20180605, "ALL") 
```

*上述stock_type可填入下列value中對應的代號*
```Javascript
<option value="ALL">全部</option>
<option value="ALLBUT0999">全部(不含權證、牛熊證、可展延牛熊證)</option>
<option value="0049">封閉式基金</option>
<option value="0099P">ETF</option>
<option value="019919T">受益證券</option>
<option value="0999">認購權證(不含牛證)</option>
<option value="0999P">認售權證(不含熊證)</option>
.
.
.
<option value="01" selected="">水泥工業</option>
<option value="02">食品工業</option>
<option value="03">塑膠工業</option>
<option value="04">紡織纖維</option>
<option value="05">電機機械</option>
<option value="06">電器電纜</option>
.
.
.
(其餘可至證券交易所官網查詢)
```

```python
#檢視main_force物件的參數，將依據此參數爬取資料
>>> main_force.params
{'date': 20180605, 'selectType': 'ALL'}
```
```python
#main_force物件中含有get_original_data()，此功能將爬取網頁上所有資料
>>> main_force_original_data = main_force.get_original_data() 
>>> main_force_original_data.head()
```

<table border="1" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>證券代號</th>      <th>證券名稱</th>      <th>外陸資買進股數(不含外資自營商)</th>      <th>外陸資賣出股數(不含外資自營商)</th>      <th>外陸資買賣超股數(不含外資自營商)</th>      <th>外資自營商買進股數</th>      <th>外資自營商賣出股數</th>      <th>外資自營商買賣超股數</th>      <th>投信買進股數</th>      <th>投信賣出股數</th>      <th>投信買賣超股數</th>      <th>自營商買賣超股數</th>      <th>自營商買進股數(自行買賣)</th>      <th>自營商賣出股數(自行買賣)</th>      <th>自營商買賣超股數(自行買賣)</th>      <th>自營商買進股數(避險)</th>      <th>自營商賣出股數(避險)</th>      <th>自營商買賣超股數(避險)</th>      <th>三大法人買賣超股數</th>      <th>日期</th>    </tr>  </thead>  <tbody>    <tr>      <th>0</th>      <td>00637L</td>      <td>元大滬深300正2</td>      <td>18,098,000</td>      <td>3,984,000</td>      <td>14,114,000</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>16,906,000</td>      <td>480,000</td>      <td>700,000</td>      <td>-220,000</td>      <td>33,575,000</td>      <td>16,449,000</td>      <td>17,126,000</td>      <td>31,020,000</td>      <td>2018-06-05</td>    </tr>    <tr>      <th>1</th>      <td>2344</td>      <td>華邦電</td>      <td>26,176,500</td>      <td>6,651,452</td>      <td>19,525,048</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>-2,112,000</td>      <td>328,000</td>      <td>750,000</td>      <td>-422,000</td>      <td>1,962,000</td>      <td>3,652,000</td>      <td>-1,690,000</td>      <td>17,413,048</td>      <td>2018-06-05</td>    </tr>    <tr>      <th>2</th>      <td>2448</td>      <td>晶電</td>      <td>11,871,000</td>      <td>4,920,000</td>      <td>6,951,000</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>1,757,000</td>      <td>2,000,000</td>      <td>-243,000</td>      <td>7,235,000</td>      <td>2,258,000</td>      <td>687,000</td>      <td>1,571,000</td>      <td>6,578,000</td>      <td>914,000</td>      <td>5,664,000</td>      <td>13,943,000</td>      <td>2018-06-05</td>    </tr>    <tr>      <th>3</th>      <td>2890</td>      <td>永豐金</td>      <td>12,210,000</td>      <td>1,475,445</td>      <td>10,734,555</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>634,000</td>      <td>0</td>      <td>634,000</td>      <td>-756,000</td>      <td>4,000</td>      <td>49,000</td>      <td>-45,000</td>      <td>10,000</td>      <td>721,000</td>      <td>-711,000</td>      <td>10,612,555</td>      <td>2018-06-05</td>    </tr>    <tr>      <th>4</th>      <td>2885</td>      <td>元大金</td>      <td>19,244,000</td>      <td>9,244,000</td>      <td>10,000,000</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>0</td>      <td>552,989</td>      <td>1,000</td>      <td>0</td>      <td>1,000</td>      <td>1,099,989</td>      <td>548,000</td>      <td>551,989</td>      <td>10,552,989</td>      <td>2018-06-05</td>    </tr>  </tbody></table>

``` python
#由於只需要網頁資料裡的一部份，因此預設selected_columns存放留取的欄位
>>> main_force.selected_columns
['日期', '證券代號', '證券名稱', '外陸資買賣超股數(不含外資自營商)', '投信買賣超股數']
```

```python
#因此get_selected_data()將依據上述selected_columns爬取指定之欄位
#可透過更改selected_columns此屬性來更改get_selected_data()爬取之資料欄位
>>> main_force_selected_data = main_force.get_selected_data()
>>> main_force_selected_data.head()
```

<table border="1" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>日期</th>      <th>股票代號</th>      <th>股票名稱</th>      <th>外陸資買賣超股數(不含外資自營商)</th>      <th>投信買賣超股數</th>    </tr>  </thead>  <tbody>    <tr>      <th>1</th>      <td>2018-06-05</td>      <td>2344</td>      <td>華邦電</td>      <td>19525.048</td>      <td>0.0</td>    </tr>    <tr>      <th>2</th>      <td>2018-06-05</td>      <td>2448</td>      <td>晶電</td>      <td>6951.000</td>      <td>-243.0</td>    </tr>    <tr>      <th>3</th>      <td>2018-06-05</td>      <td>2890</td>      <td>永豐金</td>      <td>10734.555</td>      <td>634.0</td>    </tr>    <tr>      <th>4</th>      <td>2018-06-05</td>      <td>2885</td>      <td>元大金</td>      <td>10000.000</td>      <td>0.0</td>    </tr>    <tr>      <th>5</th>      <td>2018-06-05</td>      <td>2884</td>      <td>玉山金</td>      <td>10346.716</td>      <td>-500.0</td>    </tr>  </tbody></table>

* **融資融券**
功能基本上與三大法人買賣超相同，不同之處在於目前此物件無法透過更改 *selected_columns* 此屬性以 *get_selected_data()* 爬取欲選擇的欄位，因為融資融券資料並沒有欄位提供當日總變動量，需要在爬取下來後由今日餘額減去昨日餘額計算，由於這裡需要當日變動的資料，因此這部分已寫死在 *get_selected_data()* 當中，但仍可以透過 *get_original_data* 擷取自己需要的部分。

``` python
#建立margin_purchase_short_sell物件，此物件基本功能與三大法人相同，因此以下僅呈現不同之處
margin_purchase_short_sell = Exchange_margin_purchase_short_sell(20180605)

#爬取selected_data，此部分無法透過更改selected_columns來修改欲選取欄位
margin_purchase_short_sell_selected_data = margin_purchase_short_sell.get_selected_data()
margin_purchase_short_sell_selected_data.head()
```
<table border="1" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>日期</th>      <th>股票代號</th>      <th>股票名稱</th>      <th>融資張數</th>      <th>融券張數</th>    </tr>  </thead>  <tbody>    <tr>      <th>0</th>      <td>2018-06-05</td>      <td>0050</td>      <td>元大台灣50</td>      <td>-55.0</td>      <td>0.0</td>    </tr>    <tr>      <th>1</th>      <td>2018-06-05</td>      <td>0051</td>      <td>元大中型100</td>      <td>0.0</td>      <td>0.0</td>    </tr>    <tr>      <th>2</th>      <td>2018-06-05</td>      <td>0052</td>      <td>富邦科技</td>      <td>0.0</td>      <td>0.0</td>    </tr>    <tr>      <th>3</th>      <td>2018-06-05</td>      <td>0053</td>      <td>元大電子</td>      <td>0.0</td>      <td>0.0</td>    </tr>    <tr>      <th>4</th>      <td>2018-06-05</td>      <td>0054</td>      <td>元大台商50</td>      <td>0.0</td>      <td>0.0</td>    </tr>  </tbody></table>

* **借券**
借券部分與融資融券相同，皆需要計算當日變動量(已寫死)，因此無法透過更改 *selected_columns* 選取欲爬取之欄位，但仍可以透過 *get_original_data* 擷取自己需要的部分，其餘部分大致與融資融券相同。

```python
#建立borrow物件
borrow = Exchange_borrow(20180605)

#爬取selected_data，此部分無法透過更改selected_columns來修改欲選取欄位
borrow_selected_data = borrow.get_selected_data()
borrow_selected_data.head()
```

<table border="1" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>日期</th>      <th>股票代號</th>      <th>股票名稱</th>      <th>借券張數</th>    </tr>  </thead>  <tbody>    <tr>      <th>0</th>      <td>2018-06-05</td>      <td>0050</td>      <td>元大台灣50</td>      <td>-4.0</td>    </tr>    <tr>      <th>1</th>      <td>2018-06-05</td>      <td>0051</td>      <td>元大中型100</td>      <td>0.0</td>    </tr>    <tr>      <th>2</th>      <td>2018-06-05</td>      <td>0052</td>      <td>富邦科技</td>      <td>0.0</td>    </tr>    <tr>      <th>3</th>      <td>2018-06-05</td>      <td>0053</td>      <td>元大電子</td>      <td>0.0</td>    </tr>    <tr>      <th>4</th>      <td>2018-06-05</td>      <td>0054</td>      <td>元大台商50</td>      <td>0.0</td>    </tr>  </tbody></table>

* **deal_with_data 物件**
1. 此物件將上述三大法人、融資融券、借券等物件各別的 get_selected_data()結果，分別指定至 *deal_with_data* 物件中的 *main_force, margin_purchase_short_sell, borrow* 等屬性內，接著透過 *deal_with_data* 物件中的 *get_today_data()* 獲得上述三者合併後的資料，此處 *deal_with_data* 可填入任意日期，若不填入則預設為今日。
2. *calculate_indicator()* 則將整理後的資料迭代回累積資料後，接著計算自定義的指標，此處的指標計算方式為[(外資買賣超+投信買賣超-借券)-(融資-融券)]/(流通在外張數-董監事持股張數)，此策略著重於外資與投信等偏長線操作的法人，並將散戶視為反指標扣除散戶做多的部分，因此每日將計算出一個百分比，接著透過累加方式，計算以當日為基準回推5日、10日、20日、60日累加的結果，透過觀察此累加值便可得知法人近期操作趨勢，計算結果將分為四張表，第二至第四張表分別為5日、20日、60日指標由大致小排序取前30名，第一張表則為上述三張表中20日指標大過於60日指標者，亦即近期買超較多者。
3. *transform_to_html* 則將 *calculate_indicator()* 計算後的結果轉成HTML的格式，最後則透過另外一個物件gmail將結果寄出。

```python
#建立exchange_deal_with_data物件
exchange_deal_with_data = Exchange_deal_with_data(20180605)

#獲取今日完整資料
exchange_today_data = exchange_deal_with_data.get_today_data()
exchange_today_data.head()
```

<table border="1" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>日期</th>      <th>股票代號</th>      <th>股票名稱</th>      <th>外陸資買賣超股數(不含外資自營商)</th>      <th>投信買賣超股數</th>      <th>借券張數</th>      <th>融資張數</th>      <th>融券張數</th>    </tr>  </thead>  <tbody>    <tr>      <th>0</th>      <td>2018-06-05</td>      <td>2344</td>      <td>華邦電</td>      <td>19525.048</td>      <td>0.0</td>      <td>426.0</td>      <td>-2556.0</td>      <td>296.0</td>    </tr>    <tr>      <th>1</th>      <td>2018-06-05</td>      <td>2448</td>      <td>晶電</td>      <td>6951.000</td>      <td>-243.0</td>      <td>-438.0</td>      <td>-1395.0</td>      <td>1506.0</td>    </tr>    <tr>      <th>2</th>      <td>2018-06-05</td>      <td>2890</td>      <td>永豐金</td>      <td>10734.555</td>      <td>634.0</td>      <td>-49.0</td>      <td>-80.0</td>      <td>-1.0</td>    </tr>    <tr>      <th>3</th>      <td>2018-06-05</td>      <td>2885</td>      <td>元大金</td>      <td>10000.000</td>      <td>0.0</td>      <td>121.0</td>      <td>-981.0</td>      <td>6.0</td>    </tr>    <tr>      <th>4</th>      <td>2018-06-05</td>      <td>2884</td>      <td>玉山金</td>      <td>10346.716</td>      <td>-500.0</td>      <td>-55.0</td>      <td>-85.0</td>      <td>9.0</td>    </tr>  </tbody></table>

```python
#計算今日自定義指標
exchange_today_indactor = exchange_deal_with_data.calculate_indicator()
exchange_today_indactor.head()
```
<table border="1" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>日期</th>      <th>股票代號</th>      <th>股票名稱</th>      <th>外陸資買賣超張數</th>      <th>投信買賣超張數</th>      <th>借券張數</th>      <th>融資張數</th>      <th>融券張數</th>      <th>當日指標餘額</th>      <th>近5日指標比例</th>      <th>近10日指標比例</th>      <th>近20日指標比例</th>      <th>近60日指標比例</th>    </tr>  </thead>  <tbody>    <tr>      <th>92411</th>      <td>2018-06-05</td>      <td>2014</td>      <td>中鴻</td>      <td>888</td>      <td>-700</td>      <td>14</td>      <td>0</td>      <td>0</td>      <td>174</td>      <td>0.008</td>      <td>0.010</td>      <td>0.006</td>      <td>-0.026</td>    </tr>    <tr>      <th>92387</th>      <td>2018-06-05</td>      <td>3481</td>      <td>群創</td>      <td>2344.73</td>      <td>294</td>      <td>-32758</td>      <td>-688</td>      <td>-17</td>      <td>36067.7</td>      <td>0.008</td>      <td>0.005</td>      <td>0.014</td>      <td>-0.003</td>    </tr>    <tr>      <th>92509</th>      <td>2018-06-05</td>      <td>6120</td>      <td>達運</td>      <td>-156</td>      <td>200</td>      <td>294</td>      <td>250</td>      <td>-26</td>      <td>-526</td>      <td>-0.024</td>      <td>-0.038</td>      <td>-0.041</td>      <td>-0.040</td>    </tr>    <tr>      <th>92358</th>      <td>2018-06-05</td>      <td>2344</td>      <td>華邦電</td>      <td>19525</td>      <td>0</td>      <td>426</td>      <td>-2556</td>      <td>296</td>      <td>21951</td>      <td>0.006</td>      <td>-0.003</td>      <td>-0.011</td>      <td>-0.055</td>    </tr>    <tr>      <th>92400</th>      <td>2018-06-05</td>      <td>2618</td>      <td>長榮航</td>      <td>1739.76</td>      <td>0</td>      <td>144</td>      <td>-1891</td>      <td>0</td>      <td>3486.76</td>      <td>0.003</td>      <td>0.002</td>      <td>0.002</td>      <td>0.016</td>    </tr>  </tbody></table>

``` python
#將calculate_indicator()的結果轉成HTML格式(以下就不呈現了轉換後的結果了)
exchange_deal_with_data.transform_to_html()
```

* **gmail物件**
透過 *gmail* 物件將 *deal_with_data* 中的 *transform_to_html()* 結果寄出。gmail物件中首先填入接收者清單，接著填入 *deal_with_data* 物件，此處設計為 *gmail* 物件建立後，將會同時建立 *deal_with_data* 此物件， *deal_with_data* 物件建立後將會進行爬取、儲存、計算、轉成HTML等步驟，最後 *gmail* 物件透過 *send_gmail()* 把HTML內容，傳送給接收者清單的每位接收者。

```python
#建立接收者清單，需為array
receivers_email_list = ["andy566159@gmail.com", "example@gmail.com"]

#傳送上櫃計算結果
gmail(receivers_email_list, OTC_deal_with_data()).send_gmail()

#傳送上市計算結果
gmail(receivers_email_list, Exchange_deal_with_data()).send_gmail()
```

* **自動化**
透過Windows Task Scheduler每日自動執行以下程式(程式會自動偵測當日有無開市，若無則回傳今日休市資訊)：

```python
#daily_run.py
from Exchange import *
from OTC import *

start = datetime.now()

#main
receivers_email_list = ["andy566159@gmail.com", "example@gmail.com"]
gmail(receivers_email_list, OTC_deal_with_data()).send_gmail()
gmail(receivers_email_list, Exchange_deal_with_data()).send_gmail()

time_taken = datetime.now() - start
print("{} seconds taken...".format(round(time_taken.total_seconds(), 2)))
```
![img](https://github.com/Andy-Liu66/Chip-Project/blob/master/Original/daily_run_output.PNG)

*收到的信件如下：*
![img](https://github.com/Andy-Liu66/Chip-Project/blob/master/Original/result.PNG)

* **備註**
1. 上述功能在 *get_selected_data()* 中將只會選擇出符合代碼為四位數者(股票或ETF)，排除選擇權或其他商品，但 *get_original_data()* 則保留所有商品。
2. 如果爬取資料過程出錯，則會直接回傳空的DataFrame。
3. 在計算累加指標時，由於個股在某些天理可能沒有被交易(未涵蓋在上述三大部分中)，因此將會出現空值，但由於計算累加指標時，時間為一重要考量點，因此若個股某天未被交易，則會全部補0才不會失真。
4. "2018_Exchange_temp_result.csv", "2018_OTC_temp_result.csv" 分別儲存上市與上櫃公司自2018年至今的籌碼指標(選擇過後的)。
5. "2018_outstanding_stock.csv" 則儲存2018/5/15日獲取之淨流通在外張數(流通在外張數-董監持股張數)，將會依據此csv檔中涵蓋的上市櫃公司計算指標，若此公司在此日期之前下市，則其碼資料可能含有該個股，但由於未在此csv檔案中，所以不會被計算指標，此資料由TEJ資料庫下載後整理而得。
6. 證交所與櫃台買賣中心資料公布時間不一定，有時下午4, 5點便公布，有時則晚上9, 10點。
7. 太頻繁爬取可能會被擋，需爬取多筆歷史資料時建議使用time.sleep()拉長間隔時間。

* **FeedBack:**

此程式只是個人用途，因此當中許多部份是寫死的沒有彈性，但是個別模組中的 *get_original_data()* 則保留網頁所有資料，因此可以再將其包成其他function使用，歡迎提點任何改進之處，或寄信至andy566159@gmail.com。