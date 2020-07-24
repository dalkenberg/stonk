from stonklib import *

os.environ["data_folder"] = "data"
pd.set_option("display.max_rows", None, "display.max_columns", None) # for debugging

def update_all_data(start_date, symbol_list):
    sl = stonklib(start_date)
    catchup_status = sl.catchup(symbol_list)
    print("stock data all caught up" if catchup_status.lower() == "complete" else "error in stock data catchup function")

dt = datetime.now().strftime("%Y-%m-%d")
start_date = "2020-01-01"
symbol_list = ["SPY", "AAPL", "MSFT", "TSLA", "T", "NKE", "DIS"]
#symbol_list = None

update_all_data(start_date, symbol_list)
