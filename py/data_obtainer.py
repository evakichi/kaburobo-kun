import requests
import os
import json
from multiprocessing import Process,Queue
import pandas as pd
import datetime as dt
from datetime import timedelta
import mplfinance as mpf
import japanize_matplotlib  
import pathlib
import ssl
import shutil
from smtplib import SMTP_SSL
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
from email.mime.application import MIMEApplication

class TokenTaker:

    def __init__(self) -> None:
        self.user = os.environ.get('J_QUANTS_USER')
        self.passwd = os.environ.get('J_QUANTS_PASSWD')
        self.id_token = self.get()

    def get(self) -> str:
        data = {"mailaddress": self.user, "password": self.passwd}
        refresh_token_taker = requests.post(
            "https://api.jquants.com/v1/token/auth_user", data=json.dumps(data))
        refresh_token = refresh_token_taker.json()['refreshToken']

        id_token_taker = requests.post(
            f"https://api.jquants.com/v1/token/auth_refresh?refreshtoken={refresh_token}")
        id_token = id_token_taker.json()['idToken']

        return id_token

class BrandTaker:

    def __init__(self,id_token):
        self.brands = self.get(id_token)

    def get(self,id_token):
        headers = {'Authorization': f"Bearer {id_token}"}
        requests_obtainer = requests.get("https://api.jquants.com/v1/listed/info", headers=headers)
        return requests_obtainer.json()['info']

class CalenderTaker:
    
    def __init__(self,id_token):
        self.id_token = id_token
        self.get(id_token)

    def get(self,id_token):
        headers = {'Authorization': f'Bearer {id_token}'}
        self.calendar = requests.get(f"https://api.jquants.com/v1/markets/trading_calendar", headers=headers).json()

class QuantsTaker:

    def __init__(self,id_token,calendar,brands):
        self.id_token = id_token
        self.brands = brands
        self.calendar = calendar
        self.num_of_threads = int(os.getenv('NUM_OF_THREADS'))

        self.get(id_token,calendar,brands)
        
    def get(self,id_token,calendar,brands):
        self.packages = list()
        code_length = len(brands)
        #for outer_iter in range(code_length//self.num_of_threads):
        for outer_iter in range(10):
            process_list = list()
            queue_list = list()
            for iter in range(self.num_of_threads):
                current_iter = outer_iter*self.num_of_threads+iter
                queue = Queue()
                process_list.append(Process(target=self.get_in_parallel,args=(id_token,current_iter,calendar,brands[current_iter],queue)))
                queue_list.append(queue)
            [p.start() for p in process_list]
            [self.packages.append(q.get()) for q in queue_list]
            [p.join() for p in process_list]

        process_list = list()
        queue_list = list()
        for iter in range(self.num_of_threads*(code_length//self.num_of_threads),code_length):
            queue=Queue()
            process_list.append(Process(target=self.get_in_parallel,args=(id_token,iter,calendar,brands[iter],queue)))
            queue_list.append(queue)
        [p.start() for p in process_list]
        [self.packages.append(q.get()) for q in queue_list]
        [p.join() for p in process_list]

    def get_in_parallel(self,id_token,num,calendar,brand,queue):
        brand_code = brand["Code"]
        print("get in parallel",num,brand_code)
        headers = {'Authorization': f'Bearer {id_token}'}
        daily_quotes_json = requests.get(f"https://api.jquants.com/v1/prices/daily_quotes?code={brand_code}", headers=headers).json()
        daily_quotes = daily_quotes_json["daily_quotes"]
        date = list()
        open = list()
        high = list()
        low = list()
        close = list()
        upper_limit = list()
        lower_limit = list()
        volume = list()
        turnover_value = list()
        adjustment_factor = list()
        adjustment_open = list()
        adjustment_high = list()
        adjustment_low = list()
        adjustment_close = list()
        adjustment_volume = list()

        for cal in calendar['trading_calendar']:
            if cal['HolidayDivision'] not in ["0","3"]:
                date.append(cal["Date"])
                flag = False
                for daily_quote in daily_quotes:
                    if cal["Date"] == daily_quote["Date"]:
                        open.append(daily_quote["Open"])
                        high.append(daily_quote["High"])
                        low.append(daily_quote["Low"])
                        close.append(daily_quote["Close"])
                        upper_limit.append(daily_quote["UpperLimit"])
                        lower_limit.append(daily_quote["LowerLimit"])
                        volume.append(daily_quote["Volume"])
                        turnover_value.append(daily_quote["TurnoverValue"])
                        adjustment_factor.append(daily_quote["AdjustmentFactor"])
                        adjustment_open.append(daily_quote["AdjustmentOpen"])
                        adjustment_high.append(daily_quote["AdjustmentHigh"])
                        adjustment_low.append(daily_quote["AdjustmentLow"])
                        adjustment_close.append(daily_quote["AdjustmentClose"])
                        adjustment_volume.append(daily_quote["AdjustmentVolume"])
                        flag = True
                if not flag:
                    open.append(None)
                    high.append(None)
                    low.append(None)
                    close.append(None)
                    upper_limit.append(None)
                    lower_limit.append(None)
                    volume.append(None)
                    turnover_value.append(None)
                    adjustment_factor.append(None)
                    adjustment_open.append(None)
                    adjustment_high.append(None)
                    adjustment_low.append(None)
                    adjustment_close.append(None)
                    adjustment_volume.append(None)
        data = {"Date":date,"Open":open,"High":high,"Low":low,"Close":close,"UpperLimit":upper_limit,"LowerLimit":lower_limit,"Volume":volume,
                "TurnoverValue":turnover_value,"AdjustmentFactor":adjustment_factor,
                "AdjustmentOpen":adjustment_open,"AdjustmentHigh":adjustment_high,
                "AdjustmentLow":adjustment_low,"AdjustmentClose":adjustment_close,"AdjustmentVolume":adjustment_volume}
        df = pd.DataFrame(data,columns=["Date","Open","High","Low","Close","UpperLimit","LowerLimit","Volume","TurnoverValue","AdjustmentFactor","AdjustmentOpen","AdjustmentHigh",
                "AdjustmentLow","AdjustmentClose","AdjustmentVolume"])
        df["Date"] = pd.to_datetime(df["Date"])
        package = {"Brand":brand,"Data":df}
        queue.put(package)



class Main:
    
    def __init__(self):
        token_taker = TokenTaker()
        brand_taker = BrandTaker(token_taker.id_token)
        calender_taker = CalenderTaker(token_taker.id_token)
        quants_taker = QuantsTaker(token_taker.id_token,calender_taker.calendar,brand_taker.brands)
        packages = quants_taker.packages

        today = dt.datetime.today()
        #print(today)
        if today.hour <= 18:
            today = today - timedelta(days=1)

        working_day = list()
        for cal in calender_taker.calendar["trading_calendar"]:
            if cal["HolidayDivision"] not in ["0","3"]:
                working_day.append(dt.datetime.strptime(cal["Date"], '%Y-%m-%d'))
        current = today
        for day in working_day:
            if day <= today:
                previous = current
                current = day
        print(current,previous)
        self.sell_timing = list()
        self.buy_timing = list()
        for package in packages:
            df = package["Data"]
            df["Rolling5"] = df["Close"].fillna(0).rolling(5).mean()
            df["Rolling25"] = df["Close"].fillna(0).rolling(25).mean()
            df["Rolling150"] = df["Close"].fillna(0).rolling(150).mean()
            #print(tmp_df)
            df = df[df["Date"] <= current]
            df = df[df["Date"] >= previous]
            #print(df)
            df = df.dropna(how="any")
            if df.shape[0] == 2:
                #print(package["Code"])
                df = df.set_index("Date")
                if df.loc[current]["Rolling25"] > df.loc[current]["Close"] and df.loc[previous]["Close"] > df.loc[previous]["Rolling25"]:
                    slope_rolling25 = df.loc[current]["Rolling25"]-df.loc[previous]["Rolling25"]
                    slope_rolling5 = df.loc[current]["Rolling5"]-df.loc[previous]["Rolling5"]
                    slope_close = df.loc[current]["Close"]-df.loc[previous]["Close"]
                    index = (slope_rolling25 - slope_close)/((df.loc[current]["Volume"]+df.loc[previous]["Volume"])/2)
                    index = index / df.loc[current]["Close"]
                    buy_timing = {"BrandCode":package["Brand"]["Code"],"Index":index,"Close":df.loc[current]["Close"],"Data":package["Data"]}
                    self.buy_timing.append(buy_timing)
                    #print("Sell timing",index)
                    #print(df)
                if df.loc[current]["Close"] > df.loc[current]["Rolling25"] and df.loc[previous]["Rolling25"] > df.loc[previous]["Close"]:
                    slope_rolling25 = df.loc[current]["Rolling25"]-df.loc[previous]["Rolling25"]
                    slope_rolling5 = df.loc[current]["Rolling5"]-df.loc[previous]["Rolling5"]
                    slope_close = df.loc[current]["Close"]-df.loc[previous]["Close"]
                    index = (slope_close - slope_rolling25)/((df.loc[current]["Volume"]+df.loc[previous]["Volume"])/2)
                    index = index / df.loc[current]["Close"]
                    sell_timing = {"BrandCode":package["Brand"]["Code"],"Index":index,"Close":df.loc[current]["Close"],"Data":package["Data"]}
                    self.sell_timing.append(sell_timing)
                    #print("Buy timing",index)
                    #print(df)
            else:
                pass
                #print(package["Code"]+" is not a target")
        
        #print("Golden Cross")
        brand_code = list()
        index = list()
        close = list()
        for sell_timing in self.sell_timing:
            brand_code.append(sell_timing["BrandCode"])
            index.append(sell_timing["Index"])
            close.append(sell_timing["Close"])
        data = {"BrandCode":brand_code,"Index":index,"Close":close}
        sell_timing_df = pd.DataFrame(data,columns=["BrandCode","Index","Close"])
        sell_timing_df = sell_timing_df.sort_values("Index", ascending=False)
        sell_timing_less_than_500 = sell_timing_df[sell_timing_df["Close"]< 500.0].head(3)
        sell_timing_between_500_and_3000 = sell_timing_df[sell_timing_df["Close"] >= 500.0]
        sell_timing_between_500_and_3000 = sell_timing_between_500_and_3000[sell_timing_between_500_and_3000["Close"] < 3000.0].head(3)
        sell_timing_between_3000_and_30000 = sell_timing_df[sell_timing_df["Close"] >= 3000.0]
        sell_timing_between_3000_and_30000 = sell_timing_between_3000_and_30000[sell_timing_between_3000_and_30000["Close"] < 30000.0].head(3)
        sell_timing_over_30000 = sell_timing_df[sell_timing_df["Close"] >= 30000.0].head(3)
        print("Sell timing < 500.0")
        print(sell_timing_less_than_500)
        print("500 <= Sell timing < 3000.0")
        print(sell_timing_between_500_and_3000)
        print("3000.0 <= Sell timing < 30000.0")
        print(sell_timing_between_3000_and_30000)
        print("Sell timing >= 30000.0")
        print(sell_timing_over_30000)

        #print("Dead Cross")
        brand_code = list()
        index = list()
        close = list()
        for buy_timing in self.buy_timing:
            brand_code.append(buy_timing["BrandCode"])
            index.append(buy_timing["Index"])
            close.append(buy_timing["Close"])
        data = {"BrandCode":brand_code,"Index":index,"Close":close}
        buy_timing_df = pd.DataFrame(data,columns=["BrandCode","Index","Close"])
        buy_timing_df = buy_timing_df.sort_values("Index", ascending=False)
        buy_timing_less_than_500 = buy_timing_df[buy_timing_df["Close"]< 500.0].head(3)
        buy_timing_between_500_and_3000 = buy_timing_df[buy_timing_df["Close"] >= 500.0]
        buy_timing_between_500_and_3000 = buy_timing_between_500_and_3000[buy_timing_between_500_and_3000["Close"] < 3000.0].head(3)
        buy_timing_between_3000_and_30000 = buy_timing_df[buy_timing_df["Close"] >= 3000.0]
        buy_timing_between_3000_and_30000 = buy_timing_between_3000_and_30000[buy_timing_between_3000_and_30000["Close"] < 30000.0].head(3)
        buy_timing_over_30000 = buy_timing_df[buy_timing_df["Close"] >= 30000.0].head(3)
        print("Buy timing < 500.0")
        print(buy_timing_less_than_500)
        print("500.0 <= Buy timing < 3000.0")
        print(buy_timing_between_500_and_3000)
        print("3000.0 <= Buy timing < 30000.0")
        print(buy_timing_between_3000_and_30000)
        print("Buy timing >= 30000.0")
        print(buy_timing_over_30000)
        now = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        dir_path = pathlib.Path(os.path.join("/tmp/",now))
        dir_path.mkdir()

        results = list()
        result = {"Kind":"500円未満の買い時銘柄TOP3","KindEnglish":"Buy timing less than 500 Yen","Data":buy_timing_less_than_500}
        results.append(result)
        result = {"Kind":"500円以上3,000円未満の買い時銘柄TOP3","KindEnglish":"Buy timing between 500 and 3,000 Yen","Data":buy_timing_between_500_and_3000}
        results.append(result)
        result = {"Kind":"3,000円以上30,000円未満の買い時銘柄TOP3","KindEnglish":"Buy timing between 3,000 and 30,000 Yen","Data":buy_timing_between_3000_and_30000}
        results.append(result)
        result = {"Kind":"30,000円以上の買い時銘柄TOP3","KindEnglish":"Buy timing over 30,000 Yen","Data":buy_timing_over_30000}
        results.append(result)

        result = {"Kind":"500円未満の売り時銘柄TOP3","KindEnglish":"Sell timing less than 500 Yen","Data":sell_timing_less_than_500}
        results.append(result)
        result = {"Kind":"500円以上3,000円未満の売り時銘柄TOP3","KindEnglish":"Sell timing between 500 and 3,000 Yen","Data":sell_timing_between_500_and_3000}
        results.append(result)
        result = {"Kind":"3,000円以上30,000円未満の売り時銘柄TOP3","KindEnglish":"Sell timing between 3,000 and 30,000 Yen","Data":sell_timing_between_3000_and_30000}
        results.append(result)
        result = {"Kind":"30,000円以上の売り時銘柄TOP3","KindEnglish":"Sell timing over 30,000 Yen","Data":sell_timing_over_30000}
        results.append(result)

        self.print_image(dir_path,packages,current,results)
        self.send_mail(dir_path,packages,current,results)

    def print_image(self,dir_path,packages,current,results):
        three_monthes_ago = current - timedelta(days=90)
        print(current,three_monthes_ago)
        plotstyle = mpf.make_mpf_style(base_mpf_style='default', rc={"font.family":'IPAexGothic',"font.size":'8'})
        for result in results:
            print(result["Kind"])
            brand_codes = result["Data"]["BrandCode"]
            for brand_code in brand_codes:
                for package in packages:
                    if package["Brand"]["Code"]==brand_code:
                        df = package["Data"]
                        df = df[df["Date"] <= current]
                        df = df[df["Date"] > three_monthes_ago]
                        df = df.set_index("Date")
                        series = [mpf.make_addplot(df["Close"],type="line",markersize=0.5)]
                        file_path = os.path.join(dir_path,package["Brand"]["Code"]+"_"+package["Brand"]["CompanyNameEnglish"].replace('/','_')+".png")
                        mpf.plot(df,type="candle",volume=True,mav=(5,25),title=package["Brand"]["CompanyName"],addplot=series,style=plotstyle,savefig=dict(fname=file_path,dpi=800))


    def send_mail(self,dir_path,packages,current,results):
        
        message_header = MIMEMultipart()
        message_header["Subject"] = current.strftime("%Y-%m-%d-%H:%M:%S")+"の株価情報"
        message_header["From"] = os.environ.get('KABUROBO_KUN_MAIL_FROM')
        message_header["To"] = os.environ.get('KABUROBO_KUN_MAIL_TO')
        message_header['Date'] = formatdate()

        message = ""
        for result in results:
            message += f"{result['Kind']}({result['KindEnglish']})\n"
            data = result["Data"]
            print(data)
            if data.shape[0] == 0:
                message += "該当無し。\n"
            for rank in range(data.shape[0]):
                for package in packages:
                    if package["Brand"]["Code"] == data.iat[rank,0]:
                        message += f"第{str(rank+1)}位(No. {str(rank+1)}):\n"
                        message += f"\tCode = {package['Brand']['Code']}:{package['Brand']['CompanyName']}({package['Brand']['CompanyNameEnglish']})\n\t\t Index = {data.iat[rank,1]} 前回の終値(previous close) = {data.iat[rank,2]}\n"
            message += "\n"

        print (message)

        message_text = MIMEText(message, 'plain', 'utf-8')
        message_header.attach(message_text)

        attachment_files = list()
        zip_path = str(dir_path) + '.zip'        
        shutil.make_archive(dir_path, format='zip', root_dir=dir_path)
        attachment_files.append(zip_path)
        shutil.rmtree(dir_path)


        if attachment_files:
            for attachment_file in attachment_files:
                with open(attachment_file, 'rb') as file_pointer:
                    attachment = MIMEApplication(file_pointer.read())
                    attachment.add_header(
                        "Content-Disposition",
                        "attachment",
                        filename=os.path.basename(attachment_file)
                    )
                    message_header.attach(attachment)

        #server = SMTP_SSL(os.getenv('KABUROBO_KUN_MAIL_HOST'), 465, context=ssl.create_default_context())
        #server.login(os.getenv('KABUROBO_KUN_MAIL_ACCOUNT'), os.getenv('KABUROBO_KUN_MAIL_PASSWORD'))
        #server.send_message(message_header)

        # 閉じる
        #server.quit()
main = Main()
