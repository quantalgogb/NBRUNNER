from datetime import *
import requests
import pandas as pd
from datetime import datetime, timedelta
data={}
import threading
import logging
from kiteconnect import KiteTicker
from termcolor import colored
import time
from tqdm import tqdm
import pandas as pd
from datetime import datetime
from datetime import timedelta

from multiprocessing.pool import ThreadPool


global acc_key


def history_data_cpr(ticker_val,acc_key):
    data1=['' for i in range(1)]
    start_date=(datetime.now()-timedelta(days=1029))
    new_date=(datetime.now())
    
    startd=pd.to_datetime(str(start_date)[:10])
    endd=pd.to_datetime(str(new_date))
    #print(startd,endd)
    
    url1="https://api.kite.trade/instruments/historical/"+str(ticker_val)+"/day?from="+str(startd)+"&to="+str(endd)
    #print(url1)
    HEADERS = {"X-Kite-Version": "3", "Authorization":"token pv2830q1vbrhu1eu:"+acc_key}
    res1 = requests.get(url1, headers=HEADERS)
    i=0
    data1[i] = res1.json()
    data1[i] = data1[i]["data"]["candles"]
    
    data1[i]=pd.DataFrame(data1[i])
    
    data1[i] = data1[i].rename(columns={0: 'Time', 1: 'Open', 2:'High', 3:'Low', 4:'Close', 5:'Volume'})
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    #print('History_data {} collected .'.format(ticker_val))
    return data1





def cpr_day(df):
    i=0
    enter=False
    k=1
    m=0
    global data
    data={0:{0:'1',1:'1',2:'1'}}
    x=0
    year=2020
    while(i<len(df)):
        
        if pd.to_datetime(df.Time.iloc[i]).year==year+1:
            #print('NEW-YEAR')
            k=1
            m=0
            year=year+1
            enter=False
           
        
        if pd.to_datetime(df.Time.iloc[i]).month==k and not enter:
            start=pd.to_datetime(df['Time'].iloc[i])
            m=k+1
            enter=True
            low=float('inf')
            high=float('-inf')
            low=min(low,df['Low'].iloc[i])
            high=max(high,df['High'].iloc[i])
            #print('Enter',df.Time.iloc[i],m,k)
            
        if enter and pd.to_datetime(df.Time.iloc[i]).month!=m:
            #print('on',df.Time.iloc[i])
            low=min(low,df['Low'].iloc[i])
            high=max(high,df['High'].iloc[i])
                   
            
        if enter and pd.to_datetime(df.Time.iloc[i]).month==m:
            k+=1
            m+=1
            i-=1
            enter=False
            #print('final',df.Time.iloc[i])
            low=min(low,df['Low'].iloc[i])
            high=max(high,df['High'].iloc[i])
            close=df['Close'].iloc[i]
            pivot=(high+low+close)/3
            bc=(high+low)/2
            tc=(pivot-bc)+pivot
            dt=str(start)
            st=dt[:10]+'T'+'00:00:00+0530'
            km={'Start':st,'End':df['Time'].iloc[i],'Pivot':pivot,'BC':bc,'TC':tc}
            #print(km)
            data[x]=km
            x+=1
            
        if pd.to_datetime(df.Time.iloc[i]).month==k and enter and df.Time.iloc[i]==df.Time.iloc[-1]:
            low=min(low,df['Low'].iloc[i])
            high=max(high,df['High'].iloc[i])
            close=df['Close'].iloc[i]
            pivot=(high+low+close)/3
            bc=(high+low)/2
            tc=(pivot-bc)+pivot
            dt=str(start)
            st=dt[:10]+'T'+'00:00:00+0530'
            km={'Start':st,'End':df['Time'].iloc[i],'Pivot':pivot,'BC':bc,'TC':tc}
            #print('R',km)
            data[x]=km
            x+=1
            
        
        i+=1
    

from termcolor import colored
import pandas_ta as ta
import pandas as pd
from datetime import *



from termcolor import colored
import pandas_ta as ta
import pandas as pd
from datetime import *

cpr_1day={}
cpr_1day_dict={}

def crossover(dfx,tkname,dx):
   
    dfx['Pivot']=0.0
    dfx['BC']=0.0
    dfx['TC']=0.0
    enter=False
    k=1
    x=""
    for i in range(1,len(dfx)):
        
        if k==len(data):
            break
        x=dx[k]['Start']
        
        if str(dfx['Time'].iloc[i])==x:
            #print('Enter',x)
            enter=True
            dfx['Pivot'].iloc[i]=dx[k-1]['Pivot']
            dfx['BC'].iloc[i]=dx[k-1]['BC']
            dfx['TC'].iloc[i]=dx[k-1]['TC']
        if enter and dfx['Time'].iloc[i]==dx[k]['End']:
            #print('last',dfx['Time'].iloc[i])
            dfx['Pivot'].iloc[i]=dx[k-1]['Pivot']
            dfx['BC'].iloc[i]=dx[k-1]['BC']
            dfx['TC'].iloc[i]=dx[k-1]['TC']
            #print(dfx['Pivot'].iloc[i],dfx['BC'].iloc[i])
            k+=1
            enter=False
        elif enter and dfx['Time'].iloc[i]!=dx[k]['End']:
            x=""
            #print('wk',dfx['Time'].iloc[i])
            dfx['Pivot'].iloc[i]=dx[k-1]['Pivot']
            dfx['BC'].iloc[i]=dx[k-1]['BC']
            dfx['TC'].iloc[i]=dx[k-1]['TC']
            #print(dfx['Pivot'].iloc[i],dfx['BC'].iloc[i])
    
    #cpr_1day[tkname]=df
    k={}
    k['Close']=dfx['Close'].iloc[-1]
    k['Pivot']=dfx['Pivot'].iloc[-1]
    k['BC']=dfx['BC'].iloc[-1]
    k['TC']=dfx['TC'].iloc[-1]
    
    cpr_1day_dict[tkname]=k
            
    
    
def history_data(ticker_val,acc_key):
    data1=['' for i in range(1)]
    t2=(datetime.now() - timedelta(days=0))
    t1=(datetime.now() - timedelta(days=1))
    curr_hr=str(t2).split(':')[0].split(' ')[-1]
    curr_min=str(t2).split(':')[1]
    curr_secs=str(t2).split(':')[2]
    final=curr_hr+':'+curr_min+':'+curr_secs

    t1=str(t1)
    t2=str(t2)
    t1=t1.split(" ")
    t3=t1[1].split(':')
    t3[0]='9'
    t3[1]='00'
    t3=t3[0]+':'+t3[1]+':'+t3[2]
    t1=t1[0]+"+"+t3
    t2=t2.split(" ")
    t3=t2[1].split(':')
    t3[0]='9'
    t3[1]='00'
    t3=t3[0]+':'+t3[1]+':'+t3[2]
    t2=t2[0]+"+"+final
    t1,t2

    url1="https://api.kite.trade/instruments/historical/"+str(ticker_val)+"/minute?from="+t1+"&to="+t2
    #print(url1)
    HEADERS = {"X-Kite-Version": "3", "Authorization":"token pv2830q1vbrhu1eu:"+acc_key}
    res1 = requests.get(url1, headers=HEADERS)
    i=0
    data1[i] = res1.json()
    data1[i] = data1[i]["data"]["candles"]
    data1[i]=pd.DataFrame(data1[i])
    data1[i] = data1[i].rename(columns={0: 'Time', 1: 'Open', 2:'High', 3:'Low', 4:'Close', 5:'Volume'})
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    #print('History_data {} collected .'.format(ticker_val))
    return data1




# WORKS
dicct={5633: 'ACC', 6401: 'ADANIENT', 3861249: 'ADANIPORTS', 325121: 'AMBUJACEM', 40193: 'APOLLOHOSP', 54273: 'ASHOKLEY', 60417: 'ASIANPAINT', 1510401: 'AXISBANK', 4267265: 'BAJAJ-AUTO', 4268801: 'BAJAJFINSV', 81153: 'BAJFINANCE', 579329: 'BANDHANBNK', 1195009: 'BANKBARODA', 98049: 'BEL', 108033: 'BHARATFORG', 2714625: 'BHARTIARTL', 112129: 'BHEL', 140033: 'BRITANNIA', 2763265: 'CANBK', 177665: 'CIPLA', 5215745: 'COALINDIA', 1215745: 'CONCOR', 486657: 'CUMMINSIND', 5105409: 'DEEPAKNTR', 2800641: 'DIVISLAB', 3771393: 'DLF', 225537: 'DRREDDY', 232961: 'EICHERMOT', 261889: 'FEDERALBNK', 315393: 'GRASIM', 589569: 'HAL', 1850625: 'HCLTECH', 340481: 'HDFC', 341249: 'HDFCBANK', 119553: 'HDFCLIFE', 345089: 'HEROMOTOCO', 348929: 'HINDALCO', 356865: 'HINDUNILVR', 1270529: 'ICICIBANK', 2863105: 'IDFCFIRSTB', 387073: 'INDHOTEL', 1346049: 'INDUSINDBK', 408065: 'INFY', 3484417: 'IRCTC', 424961: 'ITC', 1723649: 'JINDALSTEL', 3001089: 'JSWSTEEL', 4632577: 'JUBLFOOD', 492033: 'KOTAKBANK', 511233: 'LICHSGFIN', 2939649: 'LT', 4561409: 'LTI', 519937: 'M&M', 2815745: 'MARUTI', 582913: 'MRF', 6054401: 'MUTHOOTFIN', 2977281: 'NTPC', 633601: 'ONGC', 2730497: 'PNB', 3834113: 'POWERGRID', 4708097: 'RBLBANK', 738561: 'RELIANCE', 758529: 'SAIL', 4600577: 'SBICARD', 779521: 'SBIN', 794369: 'SHREECEM', 837889: 'SRF', 857857: 'SUNPHARMA', 871681: 'TATACHEM', 884737: 'TATAMOTORS', 877057: 'TATAPOWER', 895745: 'TATASTEEL', 2953217: 'TCS', 3465729: 'TECHM', 897537: 'TITAN', 2170625: 'TVSMOTOR', 2952193: 'ULTRACEMCO', 2889473: 'UPL', 784129: 'VEDL', 951809: 'VOLTAS', 969473: 'WIPRO', 975873: 'ZEEL'}
cpr_1day_dict={}
cpr_data={}
cpr_1day={}
url='https://api.telegram.org/bot5761331728:AAEeP9GPp69JUYXwtfw5ysP8HwugoHXZdGw/sendMessage?chat_id=-665312475&text='
flag=False
x=-1
s=""
x2=-1
rex={}
st={}
mx={}

def start(acc_key,my_api_key):
    
    global dicct,cpr_1day_dict,cpr_1day
    cpr_data={}
    tokens=list(dicct.keys())
    for tk in tqdm(tokens):
        df=history_data_cpr(tk,acc_key=acc_key)[0]
        cpr_day(df)
        cpr_data[tk]=data[list(data.keys())[-2]]
        crossover(df,dicct[tk],data)
    rex=set()
    mx=set()
    st=set()
    kws = KiteTicker(my_api_key, acc_key)
    #dicct={5633: 'ACC', 6401: 'ADANIENT', 3861249: 'ADANIPORTS', 325121: 'AMBUJACEM', 40193: 'APOLLOHOSP', 54273: 'ASHOKLEY', 60417: 'ASIANPAINT', 1510401: 'AXISBANK', 4267265: 'BAJAJ-AUTO', 4268801: 'BAJAJFINSV', 81153: 'BAJFINANCE', 579329: 'BANDHANBNK', 1195009: 'BANKBARODA', 98049: 'BEL', 108033: 'BHARATFORG', 2714625: 'BHARTIARTL', 112129: 'BHEL', 140033: 'BRITANNIA', 2763265: 'CANBK', 177665: 'CIPLA', 5215745: 'COALINDIA', 1215745: 'CONCOR', 486657: 'CUMMINSIND', 5105409: 'DEEPAKNTR', 2800641: 'DIVISLAB', 3771393: 'DLF', 225537: 'DRREDDY', 232961: 'EICHERMOT', 261889: 'FEDERALBNK', 315393: 'GRASIM', 589569: 'HAL', 1850625: 'HCLTECH', 340481: 'HDFC', 341249: 'HDFCBANK', 119553: 'HDFCLIFE', 345089: 'HEROMOTOCO', 348929: 'HINDALCO', 356865: 'HINDUNILVR', 1270529: 'ICICIBANK', 2863105: 'IDFCFIRSTB', 387073: 'INDHOTEL', 1346049: 'INDUSINDBK', 408065: 'INFY', 3484417: 'IRCTC', 424961: 'ITC', 1723649: 'JINDALSTEL', 3001089: 'JSWSTEEL', 4632577: 'JUBLFOOD', 492033: 'KOTAKBANK', 511233: 'LICHSGFIN', 2939649: 'LT', 4561409: 'LTI', 519937: 'M&M', 2815745: 'MARUTI', 582913: 'MRF', 6054401: 'MUTHOOTFIN', 2977281: 'NTPC', 633601: 'ONGC', 2730497: 'PNB', 3834113: 'POWERGRID', 4708097: 'RBLBANK', 738561: 'RELIANCE', 758529: 'SAIL', 4600577: 'SBICARD', 779521: 'SBIN', 794369: 'SHREECEM', 837889: 'SRF', 857857: 'SUNPHARMA', 871681: 'TATACHEM', 884737: 'TATAMOTORS', 877057: 'TATAPOWER', 895745: 'TATASTEEL', 2953217: 'TCS', 3465729: 'TECHM', 897537: 'TITAN', 2170625: 'TVSMOTOR', 2952193: 'ULTRACEMCO', 2889473: 'UPL', 784129: 'VEDL', 951809: 'VOLTAS', 969473: 'WIPRO', 975873: 'ZEEL'}

    url='https://api.telegram.org/bot5761331728:AAEeP9GPp69JUYXwtfw5ysP8HwugoHXZdGw/sendMessage?chat_id=-665312475&text='
    flag=False


    for i in tqdm(tokens):
        prev=history_data_cpr(i,acc_key=acc_key)[0]
        globals()['prevclose'+'_{}'.format(i)]=prev['Close'].iloc[-2]
        
        
                
    def calculate(tkname,s,ltp,ohlc):
        global rex,x,tokens,mx,flag,st,cpr_1day_dict,dicct
        cpx=cpr_1day_dict[dicct[tkname]]
        #requests.get(url+'TEST2-> TEST {} at {}'.format(dicct[tkname],ltp),stream=True)
        if x^(s.minute)==0:
            if (s.second==57 or s.second==58 or s.second==59) and tkname not in st:
                st.add(tkname)
                res={}
                res={'Ticker':dicct[tkname],'Time':s,'Close':ltp,'Pivot':cpx['Pivot'],'prev':globals()['prevclose'+'_{}'.format(tkname)]}
                #print(res)
                if globals()['prevclose'+'_{}'.format(tkname)] < max(cpx['TC'],cpx['BC']) and ltp>max(cpx['TC'],cpx['BC']):
                    print('B Prev:',globals()['prevclose'+'_{}'.format(tkname)],'max',max(cpx['TC'],cpx['BC']),'Close',ltp)
                    requests.get(url+'NBR-> BUY {} at {}'.format(dicct[tkname],ltp),stream=True)

                elif ltp<min(cpx['TC'],cpx['BC']) and globals()['prevclose'+'_{}'.format(tkname)]>max(cpx['TC'],cpx['BC']):
                    requests.get(url+'NBR-> SELL {} at {}'.format(dicct[tkname],ltp),stream=True)
                    print('S Prev:',globals()['prevclose'+'_{}'.format(tkname)],'max',max(cpx['TC'],cpx['BC']),'Close',ltp)


        else:
            x=s.minute
            #globals()['prevclose'+'_{}'.format(tkname)]=ltp
            flag=False
            st=set()
            print("-------------------------START OF {} mins-----------------------".format(x))
        
        

                
    def on_ticks(ws, ticks):
        logging.debug("Ticks: {}".format(ticks))
        tim=-1
        url='https://api.telegram.org/bot5761331728:AAEeP9GPp69JUYXwtfw5ysP8HwugoHXZdGw/sendMessage?chat_id=-665312475&text='
        x2=-1
        s=""
        count=0
        thread_list=[]
        for turn in range(len(ticks)):
            ltp=ticks[turn]['last_price']
            ohlc=ticks[turn]['ohlc']
            T2=datetime.now()
            hr2=int(T2.hour)
            min2=int(T2.minute)
            sec2=int(T2.second)
            tk=ticks[turn]['instrument_token']
            #T4=str(T2).split(' ')[0]
            TR1=str(T2).split(' ')[1]
            s=pd.to_datetime(TR1)
            try:
                t1=threading.Thread(target=calculate,args=[tk,s,ltp,ohlc])
                #t2=threading.Thread(target=calculate,args=[tk,s,ltp,ohlc])
                t1.start()
                t1.join()
            except:
                print('Error1')
                continue
            
            
    def on_connect(ws, response):
        print("Trading starting soon....")
        check_flag=False
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
        print("Daily Trading Started. Good Luck....")
        requests.get(url+'Starting NBRUNNER',stream=True)
            
                
    def on_close(ws, code, reason):

        # kws.enable_reconnect(reconnect_interval=5, reconnect_tries=50)
        # print("reconnecting...")
        requests.get(url+'Closing Connection',stream=True)
        print("closing connection")
        logging.info("Connection closed: {code} - {reason}".format(code=code, reason=reason))
        # ws.stop()

    def on_error(ws, code, reason):
        logging.info("123456Connection error: {code} - {reason}".format(code=code, reason=reason))


    def on_reconnect(ws, attempts_count):
        logging.info("123456Reconnecting: {}".format(attempts_count))


    def on_noreconnect(ws):
        logging.info("123456Reconnect failed.")


    # Assign the callbacks.
    kws.on_ticks = on_ticks

    kws.on_close = on_close
    kws.on_error = on_error
    kws.on_connect = on_connect
    kws.on_reconnect = on_reconnect
    kws.on_noreconnect = on_noreconnect
    kws.connect(threaded=True, disable_ssl_verification=False)
