\l default.q

\d .

STOCKTICKSNAP:([sym:`symbol$()] d:`date$(); t:`time$(); m:`float$())

stocktick:{
  $[x[0] in exec sym from STOCKTICKSNAP;
   if[x[2] >= STOCKTICKSNAP[x[0]][`t]; upsert[`STOCKTICKSNAP;(x[0];x[1];x[2];x[8])]];
   upsert[`STOCKTICKSNAP;(x[0];x[1];x[2];x[8])]]}


PRECLOSE:([sym:`symbol$()] p:`float$())


indices:()!()
indices_k:sh_sz {hsym each `$x ,/:(string y) ,\: "_IC.txt"}' sh_sz_indices;

udt:([sym:`symbol$()] up:`int$(); even:`int$(); down:`int$());

read_index:{
  sym:`$-7_(1+last (string x) ss "/")_(string x);
  file_content:.j.k read1 x;
  index_content:(file_content`Content)[1];
  sym_s:string sym;
  indices[`$$[sym_s[0]="3";sym_s,".SZ";sym_s,".SH"]]:`$index_content}

{@[read_index;x;(`symbol$())!(`symbol$())]} each/: indices_k;

symbols:distinct () ,/ (value indices);

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

read_ts_day_line each symbols;

updown:{
  t::PRECLOSE lj STOCKTICKSNAP;
  {
    a:() xkey select n:count x by x from select x:signum[m-p] from t where sym in indices[x];
    b:((1;0;-1)!(0;0;0)),a[`x]!a[`n];
    `udt upsert (x;b[1];b[0];b[-1])} each key indices;

  () xkey udt}
