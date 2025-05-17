import requests
import os
import json
from multiprocessing import Process,Queue
import pandas as pd
import numpy as nu

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

    def __init__(self,id_token,calendar,brand):
        self.id_token = id_token
        self.brand = brand
        self.calendar = calendar
        self.num_of_threads = int(os.getenv('NUM_OF_THREADS'))

        self.get(id_token,calendar,brand)
        
    def get(self,id_token,calendar,brand):
        packages = list()
        brand_codes = [code['Code'] for code in brand]
        code_length = len(brand_codes)
        print(f"get {code_length} codes.")
        for outer_iter in range(code_length//self.num_of_threads):
            process_list = list()
            queue_list = list()
            for iter in range(self.num_of_threads):
                current_iter = outer_iter*self.num_of_threads+iter
                queue = Queue()
                process_list.append(Process(target=self.get_in_parallel,args=(id_token,current_iter,calendar,brand_codes[current_iter],queue)))
                queue_list.append(queue)
            [p.start() for p in process_list]
            [packages.append(q.get()) for q in queue_list]
            [p.join() for p in process_list]

        process_list = list()
        queue_list = list()
        for iter in range(self.num_of_threads*(code_length//self.num_of_threads),code_length):
            queue=Queue()
            process_list.append(Process(target=self.get_in_parallel,args=(id_token,iter,calendar,brand_codes[iter],queue)))
            queue_list.append(queue)
        [p.start() for p in process_list]
        [packages.append(q.get()) for q in queue_list]
        [p.join() for p in process_list]

    def get_in_parallel(self,id_token,num,calendar,brand_code,queue):
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
            if cal['HolidayDivision'] not in ["0"]:
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
        package = {"Code":brand_code,"Data":df}
        #print(df)
        queue.put(package)




if __name__=="__main__":
    token_taker = TokenTaker()
    brand_taker = BrandTaker(token_taker.id_token)
    calender_taker = CalenderTaker(token_taker.id_token)
    quants_taker = QuantsTaker(token_taker.id_token,calender_taker.calendar,brand_taker.brands)