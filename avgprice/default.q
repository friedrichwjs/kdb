\d .avgprice

history_data:"/home/kdb/mnt/market/stock/lday/"
root:"/home/kdb/fundamental/QuoteServer/DATA/"
dailyhalt:root,"金融终端/DailyHaltSymbol.txt"
sym_files:root ,/: ("深";"上"),' "证行情/财务数据/指数成分/" ,/: ("399106";"000001") ,\: "_IC.txt"
folders:root ,/: ("深";"上") ,\: "证行情/财务数据/所属行业/"
