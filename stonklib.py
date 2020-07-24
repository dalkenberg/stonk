import os
import logging
import math
import pandas as pd
import numpy
from datetime import datetime, timedelta, date
from glob import glob
import yfinance as yf
import pandas_market_calendars as mcal


# catchup() downloads data for the given list of stock tickers (or all as defined in

class stonklib(object):
    def __init__(self, start_date):
        self._start_date = self.get_date_obj(start_date)
        self._today = self.get_date_obj(datetime.now())
        self._special_symbols = ['PRN', 'CON']  # these tickers cause problems in python because they are reserved words
        nyse = mcal.get_calendar('NYSE')
        date_list = nyse.schedule(start_date=datetime.now().date() - timedelta(days=365*5), end_date=datetime.now().date() + timedelta(days=365*5))
        dates = mcal.date_range(date_list, frequency='1D')
        self._nyse_schedule = pd.Series([x.date() for x in dates])
        self._no_options = self.get_no_options_list()

    def get_date_obj(self, dt):
        if type(dt) == str:
            return datetime.strptime(dt, "%Y-%m-%d").date()
        elif type(dt) == date:
            return dt
        elif type(dt) == datetime:
            return dt.date()
        else:
            return None

    def get_no_options_list(self):
        if len(glob("data/ticker/no_options.csv")) > 0:
            df = pd.read_csv("data/ticker/no_options.csv")
            return df['symbol'].tolist()
        else:
            return []

    def symbol_data_from_file(self, symbol):
        data_folder = os.environ["data_folder"]
        if symbol in self._special_symbols:
            file_name = f"{symbol}_X.csv"
        else:
            file_name = f"{symbol}.csv"
        if len(glob(f"{data_folder}/{file_name}")) > 0:
            df = pd.read_csv(f"{data_folder}/{file_name}", index_col=0)
            df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
            return df
        else:
            return None

    def file_data_check(self, df):
        if len(df['Date'].unique().tolist()) != len(df['Date'].tolist()):
            return False
        return True

    def get_symbol_list(self):
        symbol_df = pd.read_csv(f"data/ticker/nasdaqtraded.txt", delimiter="|")
        sym = symbol_df[~symbol_df["Symbol"].str.contains("$", regex=False)]["Symbol"]
        sym = sym[~sym.str.contains(".", regex=False)]
        return sym

    def delete_data_file(self, symbol):
        data_folder = os.environ["data_folder"]
        if symbol in self._special_symbols:
            file_name = f"{symbol}_X.csv"
        else:
            file_name = f"{symbol}.csv"
        if len(glob(f"{data_folder}/{file_name}")) > 0:
            os.remove(f"{data_folder}/{file_name}")

    def get_trading_date(self, dt, direction="forward"):
        dt = self.get_date_obj(dt)
        if direction == "forward":
            sched = pd.Series(self._nyse_schedule)
            sched = pd.Series(sched[sched >= dt])
            sched = sched.reset_index(drop=True)
            if len(sched) > 0:
                return self.get_date_obj(sched.iloc[0])
            else:
                return None
        if direction == "back":
            sched = pd.Series(self._nyse_schedule)
            sched = pd.Series(sched[sched <= dt])
            sched = sched.reset_index(drop=True)
            if len(sched) > 0:
                return self.get_date_obj(sched.iloc[-1])
            else:
                return None

    def get_current_data_situation(self, symbol_list=None):
        start_date = self._start_date
        dt_today = self.get_date_obj(datetime.now())
        if symbol_list is not None:
            symbols = symbol_list
        else:
            symbols = pd.Series(self.get_symbol_list())
        current = {}
        for symbol in symbols:
            df = self.symbol_data_from_file(symbol)
            if df is not None:
                if not self.file_data_check(df):
                    print(f"CORRUPT FILE - deleted {symbol}")
                    self.delete_data_file(symbol)
                    df = None
            if df is not None:
                first_date = df.iloc[0]['Date'].to_pydatetime().date()
                last_date = df.iloc[-1]["Date"].to_pydatetime().date()
                tup = (first_date, last_date)
                current[symbol] = tup
            # print(f"reading: {symbol}")

        status = {}
        for symbol in symbols:
            if symbol not in current.keys():
                status[symbol] = "ytd"
            else:
                first_date = self.get_date_obj(current[symbol][0])
                last_date = self.get_date_obj(current[symbol][1])

                if self.get_trading_date(start_date, "forward") < self.get_trading_date(first_date, "forward"):
                    status[symbol] = "ytd"
                elif self.get_trading_date(dt_today, "back") > self.get_trading_date(last_date, "back"):
                    days = abs((dt_today - last_date).days)
                    if (days < 6):
                        status[symbol] = f"5d"
                    elif (days < 32):
                        status[symbol] = f"1mo"
                    elif (days < 95):
                        status[symbol] = f"3mo"
                    elif (days < 185):
                        status[symbol] = f"6mo"
                    else:
                        status[symbol] = f"1yr"
                else:
                    status[symbol] = "GOOD"
            # print(f"{symbol} {status[symbol]}")
        return status

    def get_history_period(self, symbols, period):
        data = yf.download(tickers=symbols, period=period, group_by="ticker")
        data = data.reset_index()

        logging.info(f"MADE API CALL - get_historical_data({period}, {symbols})")
        print(f"MADE API CALL - get_historical_data({period}, {symbols})")
        return data

    def add_diff(self, df):
        for i in range(1, len(df)):
            df.loc[i, 'diff'] = df.loc[i, 'Close'] - df.loc[i - 1, 'Close']
            df.loc[i, 'diff_pct'] = ((df.loc[i, 'Close'] - df.loc[i - 1, 'Close']) / df.loc[i - 1, 'Close']) * 100
        return df


    def update_group(self, symbols, period):
        data_folder = os.environ["data_folder"]

        calls = math.ceil(len(symbols) / 100)
        for x in range(0, calls):
            symbol_list = symbols[x * 100:(x + 1) * 100 - 1]
            sym = ' '.join(symbol_list)

            data = self.get_history_period(sym, period)

            for symbol in symbol_list:
                try:
                    print(symbol)

                    if len(symbols) == 1:
                        d = data
                    else:
                        d = pd.concat([data["Date"], data[symbol]], axis=1)

                    if d.empty:
                        raise Exception("empty dataframe!")
                    else:

                        if len(d['Date'].unique().tolist()) != len(d['Date'].tolist()):
                            if (d.iloc[-1]['Date'] == d.iloc[-2]['Date']):
                                d.drop(data.tail(1).index, inplace=True)
                                print(f"FIXED!  {symbol}")

                        if symbol in self._special_symbols:
                            file_name = f"{symbol}_X.csv"
                        else:
                            file_name = f"{symbol}.csv"
                        d['Date'] = d['Date'].dt.date

                        df = self.symbol_data_from_file(symbol)
                        if df is not None:
                            df['Date'] = df['Date'].dt.date
                            if "diff" in df.columns:
                                df = df.drop('diff', 1)
                            if "diff_pct" in df.columns:
                                df = df.drop('diff_pct', 1)
                            if "spy_diff" in df.columns:
                                df = df.drop('spy_diff', 1)
                            last_date = df.iloc[-1]["Date"] #.to_pydatetime().date()
                        else:
                            last_date = self.get_date_obj(datetime.now()) - timedelta(days=365*5)

                        d = d.loc[d.Date > last_date]

                        df = pd.concat([df, d])
                        df = df.reset_index(drop=True)
                        if len(df) > 0:
                            #print(df)
                            df = self.add_diff(df)
                        df.to_csv(f'{data_folder}/{file_name}')
                except Exception as e:
                    print(f"Error: {str(e)}")
                    raise e

    def get_market(self, symbol_list=None):
        current = self.get_current_data_situation(symbol_list)
        print(current)
        self.update_group(pd.Series(["SPY"]), "ytd")

        periods = ["ytd", "5d", "1mo", "3mo", "6mo", "1yr"]

        for period in periods:
            symbols = [key for (key, value) in current.items() if value == period]
            self.update_group(symbols, period)


    def catchup(self, symbol_list=None):
        if type(symbol_list) == list:
            symbol_list = [x.upper() for x in symbol_list]
        self.get_market(symbol_list)
        return "complete"

