\d .

root:"/home/kdb/fundamental/QuoteServer/DATA/"
fi_folders:root ,/: ("上";"深") ,\: "证行情/财务数据/主要指标/"
ss_folders:root ,/: ("上";"深") ,\: "证行情/财务数据/股本结构/"
bi_folders:root ,/: ("上";"深") ,\: "证行情/财务数据/资产负债/"

markets:("SH";"SZ")

folders:"/home/kdb/mnt/" ,/: lower[markets] ,\: "/stock/lday/"
tradingdayinfo:"/home/kdb/fundamental/QuoteServer/DATA/金融终端/tradedayinfo.txt"
