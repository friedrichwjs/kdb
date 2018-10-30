\l default.q

\d .

STOCKFILL:([] sym:`symbol$();d:`date$();t:`time$();p:`float$();v:`int$();to:`float$();a:`int$();b:`int$())

stockfill:{`STOCKFILL insert (x[0];x[1];x[2];x[4];x[5];x[6];x[10];x[11])}

FUNDAMENTAL:([] sym:`symbol$(); NegotiableShares:`float$())

symbol_files:{[folders]
  valid_files::(();());
  files:{system"ls ",x} each folders;
  {if[x[0]="6";valid_files[0],:`$x]} each files[0];
  {if[(x[0]="3")|x[0]="0";valid_files[1],:`$x]} each files[1];
  valid_files}


fill_table:{[table;folder;fp;market]
  json_content: .j.k read1 hsym`$folder,fp;
  colname:json_content[`colName];
  linevalue:json_content[`lineValue];
  col:1_cols table;

  if[(count colname) <> count linevalue[0];:0];

  latest_index:("P"$linevalue[;1])?max "P"$linevalue[;1];
  latest_value:linevalue[latest_index];
  dict_pair:(`$colname)!latest_value;
  table insert ((`$fp[til fp?"_"],".",market),"F"$value col!dict_pair[col]);}


(0;1) {fill_table[`FUNDAMENTAL;ss_folders[x];;y] each string symbol_files[ss_folders][x]}' ("SH";"SZ");

delete valid_files from `.;

\d .ddx

ddx_value:([] sym:`symbol$();ddx:`float$())

intraday_ddx_values:([] sym:`symbol$();t1:`time$();t2:`time$();ddx:`float$())

acc_data_b:acc_data_a:([] sym:`symbol$();d:`date$();t:`time$();a:`long$();b:`long$();x:`float$())

tradable_shares:`sym xkey select sym, NegotiableShares from `.[`FUNDAMENTAL]

ddx_hdb:{[]
  fill:select from `.[`STOCKFILL] where d=2016.01.04, p<>0;
  stats:select sumv:sum(v), amount:sum(p*v) by sym, b from fill;
  thresh_data:select from stats where (sumv>=.ddx.v_thresh)|(amount>=.ddx.amount_thresh);
  select sum(amount) by sym from thresh_data}


ddx_rdb:{[start;end]
  if[0=count select from `.[`STOCKFILL] where p<>0, t>=start,t<end;:0]   / return immediately if no stock fill data yet

  ddx_rdb_acc[start;end];

  bids:select bvol_subtotal:sum(bsumv) by sym from acc_data_b where (bsumv>=.ddx.v_thresh)|(bamount>=.ddx.amount_thresh);
  asks:select avol_subtotal:sum(asumv) by sym from acc_data_a where (asumv>=.ddx.v_thresh)|(aamount>=.ddx.amount_thresh);
  t:select sym, vol:bvol_subtotal-avol_subtotal from bids + asks;

  .ddx.ddx_value:select from (select sym, ddx:vol % NegotiableShares from t lj .ddx.tradable_shares) where ddx <> 0n;
  .ddx.intraday_ddx_values ,:([] sym:ddx_value`sym; t1:start; t2:end; ddx:ddx_value`ddx);
  }

ddx_rdb_acc:{[start;end]
  valid_fills:select sym, v, to, a, b from `.[`STOCKFILL] where p<>0, t>=start, t<end;

  b_fills:select bsumv:sum(v), bamount:sum(to) by sym, b from valid_fills;
  a_fills:select asumv:sum(v), aamount:sum(to) by sym, a from valid_fills;

  $[0=count .ddx.acc_data_b;
     [.ddx.acc_data_b:`sym xkey b_fills;
      .ddx.acc_data_a:`sym xkey a_fills];
     [.ddx.acc_data_b:`sym xkey ((`sym`b xkey .ddx.acc_data_b) + b_fills);
      .ddx.acc_data_a:`sym xkey ((`sym`a xkey .ddx.acc_data_a) + a_fills)]];}

/ used to calculate historical ddx data
ddx_no_fullday:{[minutes]
  ddx_values:([] sym:`symbol$();time:`minute$(); ddx:`long$());
  `sym`time xasc ddx_values,/ddx_value_whole_market each minutes}

ddx_fullday:{[]
  minutes:(09:30+til 121),13:00+til 121;
  ddx_no_fullday[minutes]}

ddx_value_whole_market:{[time]
  valid_fills:select from .hdb.STOCKFILL0 where t<=time, p<>0;
  b_fills:() xkey select bsumv:sum(v), bamount:sum(p*v) by sym,b from valid_fills;
  a_fills:() xkey select asumv:sum(v), aamount:sum(p*v) by sym,a from valid_fills;
  b_thresh:select from b_fills where (bsumv>=.ddx.v_thresh)|(bamount>=.ddx.amount_thresh);
  a_thresh:select from a_fills where (asumv>=v_thresh)|(aamount>=amount_thresh);
  b_sum:select ddx:sum(bsumv) by sym from b_thresh;
  a_sum:select ddx:sum(asumv) by sym from a_thresh;
  ddx_v:() xkey b_sum-a_sum;
  ddx_v:update time:time from ddx_v;
  `sym`time`ddx xcols ddx_v}
