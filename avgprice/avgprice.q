\l default.q

\d .

STOCKTICK:([] sym:`symbol$(); d:`date$(); t:`time$(); m:`float$())

stocktick:{`STOCKTICK insert (x[0];x[1];x[2];x[8])}


\d .avgprice

listdates:([] sym:`symbol$(); d:`date$())

insert_symbol_listdate:{[sym_file;folder]
  symbols:((.j.k read1 hsym`$sym_file)`Content)[0][;0];
  symbol_files:`$/: folder ,/:(-3_/:symbols) ,\: "_II.txt";
  listdates:{@[{"D"$-13_((.j.k read1 x)`lineValue)[0][15]};x;0Nd]} each symbol_files;
  `.avgprice.listdates insert (`$symbols;listdates) }

sym_files insert_symbol_listdate' folders;
delete from `listdates where ((string sym) like "200*")|((string sym) like "900*");
old_syms:exec sym from listdates where 90<.z.D-d;

daily_halts:.j.k read1 hsym`$dailyhalt;
halt_syms:();
each[each[{if[x in old_syms; halt_syms,:x]}]] (`$daily_halts`SH;`$daily_halts`SZ);

PRECLOSE:([sym:`symbol$()] c:`float$())

read_ts_data_index:{{0x0 sv "x"$reverse `int$x} each 0 4 12_x}
read_ts_data_day:{{0x0 sv "x"$reverse `int$x} each 0 4 8 12 16 20 24 32 40 44_x}

read_ts_day_line:{[symbol]
  market:lower (string symbol)[7 8];
  file_names:ssr[history_data;"market";market] ,/: (string symbol)[til 1+ss[string symbol;"."][0]] ,/: ("index";"day");
  if[any {()~key hsym`$x} each file_names; :0];  / any file doesn't exist, return
  index:read_ts_data_index[read1(fp;(hcount fp:hsym`$file_names[0])-16;16)];
  if[index[2]=0i;:0];   / data length is 0, return
  day_line:read_ts_data_day[read1(hsym`$file_names[1];index[1];48)];
  close:day_line[5];
  pre_close:day_line[9];
  figure:$[close>0;close;pre_close]; / if close is 0, use pre_close
  `PRECLOSE insert (symbol;figure % 10000);
  }

read_ts_day_line each halt_syms;

ap:{[t1;t2]
  minute_table1:select c:last m by sym from `.[`STOCKTICK] where sym in .avgprice.old_syms, t>=t1,t<t2, m>0;
  minute_table:minute_table1,PRECLOSE;
  select avg c from minute_table}
