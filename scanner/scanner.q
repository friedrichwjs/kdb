\d .

rdb_template:([] sym:`symbol$(); t:`time$();latest:`float$();ask:`float$();bid:`float$())

stocktick:{$[x[0] in `rdb_template[`sym];
  update t:x[2],latest:x[8],ask:x[9],bid:x[29] from `rdb_template where sym=x[0];
  `rdb_template insert (x[0];x[2];x[8];x[9];x[29])]}


\l load_fdb.q
\l load_hdb.q


\d .scanner

merge_tables_2:{ x ij `sym xkey y };

merge_tables_3:{ x merge_tables_2/ (y;z) };

r1:{ enlist[`sym;x]!enlist[`sym;x] };
r2:{ enlist[`sym;y]!enlist[`sym;x] };
r3:{ enlist[`sym;x;y]!enlist[`sym;x;y] };

tf0:{[t;c;bo;rf0]
  t1:?[t;c;0b;r1[rf0]];
  if[bo=0;:t1];
  t2:?[t;();0b;r1[rf0]];
  ?[t2 except t1;();0b;()]}

tf:{[t;c;bo;rf1;rf2]
  t1:?[t;c;0b;r3[rf1;rf2]];
  if[bo=0;:t1];
  t2:?[t;();0b;r3[rf1;rf2]];
  ?[t2 except t1;();0b;()]}

tf1:{[t;c;bo;rf1;rf2;rf3]
  t1:?[t;c;0b;enlist[`sym;rf1;rf2;rf3]!enlist[`sym;rf1;rf2;rf3]];
  if[bo=0;:t1];
  t2:?[t;();0b;enlist[`sym;rf1;rf2;rf3]!enlist[`sym;rf1;rf2;rf3]];
  ?[t2 except t1;();0b;()]}


tc:{[t1;t2;t1_old_name;t1_new_name;t2_old_name;t2_new_name]
  common_syms:t1[`sym] inter t2[`sym];
  t11:select from t1 where sym in common_syms;
  t22:select from t2 where sym in common_syms;
  o11:?[t11;();0b;enlist[`sym;t1_new_name]!enlist[`sym;t1_old_name]];
  o22:?[t22;();0b;enlist[`sym;t2_new_name]!enlist[`sym;t2_old_name]];

  /aj[`sym;o11;o22]}
  ej[`sym;o11;o22]}


tc1:{[t1;t2;t1_old_name1;t1_new_name1;t1_old_name2;t1_new_name2;t2_old_name;t2_new_name]
  common_syms:t1[`sym] inter t2[`sym];
  t11:select from t1 where sym in common_syms;
  t22:select from t2 where sym in common_syms;
  o11:?[t11;();0b;enlist[`sym;t1_new_name1;t1_new_name2]!enlist[`sym;t1_old_name1;t1_new_name2]];
  o22:?[t22;();0b;enlist[`sym;t2_new_name]!enlist[`sym;t2_old_name]];

  /aj[`sym;o11;o22]}
  ej[`sym;o11;o22]}


tc2:{[t1;t2;t1_old_name;t1_new_name]
  common_syms:t1[`sym] inter t2[`sym];
  t11:select from t1 where sym in common_syms;
  t22:select from t2 where sym in common_syms;
  o11:?[t11;();0b;enlist[`sym;t1_new_name]!enlist[`sym;t1_old_name]];

  /aj[`sym;o11;t22]}
  ej[`sym;o11;t22]}

genhdb_rh:{[day0]
  t1:select from hdb where date=day0;
  t2:merge_tables_2[t1;rdb];
  t3:select change:latest - close,range:high - low from t2;
  t4:t2,'t3}


genhdb_rh_1:{[day0;old;new]
  t1:select from hdb where date=day0;
  t2:merge_tables_2[t1;rdb];
  t3:select change:latest - close,range:high - low from t2;
  t4:t2,'t3;
  ?[t4;();0b;enlist[`sym;new]!enlist[`sym;old]]}

display_hdb:{[f0;day0;f0d]
  t1:genhdb_rh[day0];
  ?[t1;();0b;r2[f0;f0d]]}

/<,<=,>,>=,=,<>
logic_rdb_num:{[op;topic;n]
  ?[rdb;enlist(op;topic;n);0b;r1[topic]]}

logic_rdb_rdb:{[op;topic;rf0]
  ?[rdb;enlist(op;topic;rf0);0b;r3[topic;rf0]]}

logic_rdb_hdb:{[op;topic;hf0;day0;hf0_d]
  t1:genhdb_rh[day0];

  t:tc[rdb;t1;topic;topic;hf0;hf0_d];
  ?[t;enlist(op;topic;hf0_d);0b;r3[topic;hf0_d]]}

logic_rdb_fdb:{[op;topic;ff0]
  /t:tc1[rdb;fdb;topic;topic;ff0;ff0]; the order matters, doesn't know why
  t:tc[fdb;rdb;ff0;ff0;topic;topic];
  ?[t;enlist(op;topic;ff0);();r3[topic;ff0]]}

logic_hdb_num:{[op;topic;n;day0;f0_d]
  ?[genhdb_rh[day0];enlist(op;topic;n);0b;enlist[`sym;f0_d]!enlist[`sym;topic]]}

logic_hdb_rdb:{[op;topic;rf0;day0;f0_d]
  t:tc[genhdb_rh[day0];rdb;topic;f0_d;rf0;rf0];
  ?[t;enlist(op;f0_d;rf0);0b;enlist[`sym;f0_d;rf0]!enlist[`sym;f0_d;rf0]]}


logic_hdb_hdb:{[op;topic;day0;c0;hf1;day1;c1]
  t1:genhdb_rh[day0];
  t2:genhdb_rh[day1];

  t:tc[t1;t2;topic;c0;hf1;c1];
  ?[t;enlist(op;c0;c1);0b;enlist[`sym;c0;c1]!enlist[`sym;c0;c1]]}


logic_hdb_fdb:{[op;topic;ff0;day0;hf0_d]
  t:tc[genhdb_rh[day0];fdb;topic;hf0_d;ff0;ff0];
  ?[t;enlist(op;hf0_d;ff0);0b;enlist[`sym;hf0_d;ff0]!enlist[`sym;hf0_d;ff0]]}

logic_fdb_num:{[op;topic;n]
  ?[fdb;enlist(op;topic;n);0b;r1[topic]]}

logic_fdb_rdb:{[op;topic;rf0]
  t:tc[fdb;rdb;topic;topic;rf0;rf0];
  ?[t;enlist(op;topic;rf0);0b;r2[topic;rf0]]}

logic_fdb_hdb:{[op;topic;hf0;day0;hf0_d]
  t1:genhdb_rh[day0];

  t:tc[fdb;t1;topic;topic;hf0;hf0_d];
  ?[t;enlist(op;topic;hf0_d);0b;r2[topic;hf0_d]]}

logic_fdb_fdb:{[op;topic;ff0]
  ?[fdb;enlist(op;topic;ff0);0b;r2[topic;ff0]]}

/Top#, Bottom#
rdb_head_op:{[topic;f0] f0#topic xdesc rdb}
rdb_tail_op:{[topic;f0] f0#topic xasc rdb}

ht_rdb:{[topic;f0;order]
  t:?[order=0;f0#topic xdesc rdb; f0#topic xasc rdb];
  ?[t;();0b;r1[topic]]}

ht_hdb:{[topic;f0;order;day0;f0_d]
  t:select from hdb where date=day0;
  t1:?[order=0;f0#topic xdesc t; f0#topic xasc t];
  ?[t1;();0b;enlist[`sym;f0_d]!enlist[`sym;topic]]}

ht_fdb:{[topic;f0;order]
  t:?[order=0;f0#topic xdesc fdb; f0#topic xasc fdb];
  ?[t;();0b;r1[topic]]}

/Top%, Bottom%
ht_rdb_perc:{[topic;f0;order]
  l:`int$(count rdb) * f0 % 100;
  ht_rdb[topic;l;order]}

ht_hdb_perc:{[topic;f0;order;day0;f0_d]
  t:select from hdb where date=day0;
  l:`int$(count t) * f0 % 100;
  ht_hdb[topic;l;order;day0;f0_d]}

ht_fdb_perc:{[topic;f0;order]
  l:`int$(count fdb) * f0 % 100;
  ht_fdb[topic;l;order]}

/Between, Outside

bo_rdb_num_num:{[topic;n1;n2;bo]
  c:((>=;topic;n1);(<=;topic;n2));
  tf0[rdb;c;bo;topic]}

bo_rdb_num_rdb:{[topic;n;rf0;bo]
  c:((>=;topic;n);(<=;topic;rf0));
  tf[rdb;c;bo;topic;rf0]}

bo_rdb_num_hdb:{[topic;n1;hf0;day0;bo;hf0_d]
  t:tc[rdb;genhdb_rh[day0];topic;topic;hf0;hf0_d];

  c:((>=;topic;n1);(<=;topic;hf0_d));
  tf[t;c;bo;topic;hf0_d]}

bo_rdb_num_fdb:{[topic;n1;ff0;bo]
  t:tc[rdb;fdb;topic;topic;ff0;ff0];

  c:((>=;topic;n1);(<=;topic;ff0));
  tf[t;c;bo;topic;ff0]}

bo_rdb_rdb_num:{[topic;f0;n1;bo]
  bo_rdb_num_rdb[topic;n1;f0;bo]}

bo_rdb_rdb_rdb:{[topic;rf0;rf1;bo]
  c:((>=;topic;rf0);(<=;topic;rf1));
  tf1[rdb;c;bo;topic;rf0;rf1]}

bo_rdb_rdb_hdb:{[topic;rf0;hf0;day0;bo;hf0_d]
  t:tc1[rdb;genhdb_rh[day0];topic;topic;rf0;rf0;hf0;hf0_d];

  c:((>=;topic;rf0);(<=;topic;hf0_d));
  tf1[t;c;bo;topic;rf0;hf0_d]}

bo_rdb_rdb_fdb:{[topic;rf0;ff0;bo]
  t:tc1[rdb;fdb;topic;topic;rf0;rf0;ff0;ff0];

  c:((>=;topic;rf0);(<=;topic;ff0));
  tf1[t;c;bo;topic;rf0;ff0]}

bo_rdb_hdb_num:{[topic;hf0;day0;n;bo;hf0_d]
  t:tc[rdb;genhdb_rh[day0];topic;topic;hf0;hf0_d];

  c:((>=;topic;hf0_d);(<=;topic;n));
  tf[t;c;bo;topic;hf0_d]}

bo_rdb_hdb_rdb:{[topic;hf0;day0;rf0;bo;hf0_d]
  t:tc1[rdb;genhdb_rh[day0];topic;topic;rf0;rf0;hf0;hf0_d];

  c:((>=;topic;hf0_d);(<=;topic;rf0));
  tf1[t;c;bo;topic;rf0;hf0_d]}

bo_rdb_hdb_hdb:{[topic;hf0;day0;hf1;day1;bo;hf0_d;hf1_d]
  t2:tc[genhdb_rh[day0];genhdb_rh[day1];hf0;hf0_d;hf1;hf1_d];
  t:tc1[t2;rdb;hf0_d;hf0_d;hf1_d;hf1_d;topic;topic];

  c:((>=;topic;hf0_d);(<=;topic;hf1_d));
  tf1[t;c;bo;topic;hf0_d;hf1_d]}

bo_rdb_hdb_fdb:{[topic;hf0;day0;ff0;bo;hf0_d]
  t1:tc[fdb;rdb;ff0;ff0;topic;topic];
  t2:tc1[t1;genhdb_rh[day0];topic;topic;ff0;ff0;hf0;hf0_d];

  c:((>=;topic;hf0_d);(<=;topic;ff0));
  tf1[t2;c;bo;topic;hf0_d;ff0]}

bo_rdb_fdb_num:{[topic;ff0;n;bo]
  t:tc[fdb;rdb;ff0;ff0;topic;topic];
  c:((>=;topic;ff0);(<=;topic;n));
  tf[t;c;bo;topic;ff0]}

bo_rdb_fdb_rdb:{[topic;ff0;rf1;bo]
  t1:tc[fdb;rdb;ff0;ff0;topic;topic];
  t:tc1[t1;rdb;ff0;ff0;topic;topic;rf1;rf1];
  c:((>=;topic;ff0);(<=;topic;rf1));
  tf1[t;c;bo;topic;rf1;ff0]}

bo_rdb_fdb_hdb:{[topic;ff0;hf1;day0;bo;hf1_d]
  t1:tc[fdb;rdb;ff0;ff0;topic;topic];
  t2:tc1[t1;genhdb_rh[day0];topic;topic;ff0;ff0;hf1;hf1_d];

  c:((>=;topic;ff0);(<=;topic;hf1_d));
  tf1[t2;c;bo;topic;hf1_d;ff0]}

bo_rdb_fdb_fdb:{[topic;ff0;ff1;bo]
  t2:tc[fdb;fdb;ff0;ff0;ff1;ff1];
  t:tc1[t2;rdb;ff0;ff0;ff1;ff1;topic;topic];

  c:((>=;topic;ff0);(<=;topic;ff1));
  tf1[t;c;bo;topic;ff0;ff1]}


bo_fdb_num_num:{[topic;n1;n2;bo]
  c:((>=;topic;n1);(<=;topic;n2));
  tf0[fdb;c;bo;topic]}

bo_fdb_num_rdb:{[topic;n;rf0;bo]
  t:tc[fdb;rdb;topic;topic;rf0;rf0];
  c:((>=;topic;n);(<=;topic;rf0));
  tf[t;c;bo;topic;rf0]}

bo_fdb_num_hdb:{[topic;n;hf0;day0;bo;hf0_d]
  t:tc[fdb;genhdb_rh[day0];topic;topic;hf0;hf0_d];
  c:((>=;topic;n);(<=;topic;hf0_d));
  tf[t;c;bo;topic;hf0_d]}

bo_fdb_num_fdb:{[topic;n;ff0;bo]
  c:((>=;topic;n);(<=;topic;ff0));
  tf[fdb;c;bo;topic;ff0]}

bo_fdb_rdb_num:{[topic;rf0;n;bo]
  t:tc[fdb;rdb;topic;topic;rf0;rf0];
  c:((>=;topic;rf0);(<=;topic;n));
  tf[t;c;bo;topic;rf0]}

bo_fdb_rdb_hdb:{[topic;rf0;hf1;day0;bo;hf1_d]
  t1:tc[fdb;rdb;topic;topic;rf0;rf0];
  t2:tc1[t1;genhdb_rh[day0];topic;topic;rf0;rf0;hf1;hf1_d];

  c:((>=;topic;rf0);(<=;topic;hf1_d));
  tf1[t2;c;bo;topic;hf1_d;ff0]}

bo_fdb_rdb_fdb:{[topic;rf0;ff1;bo]
  t1:tc[fdb;rdb;topic;topic;rf0;rf0];
  t:tc1[t1;fdb;topic;topic;rf0;rf0;ff1;ff1];
  c:((>=;topic;rf0);(<=;topic;rf1));
  tf1[t;c;bo;topic;rf0;ff1]}

bo_fdb_hdb_num:{[topic;hf0;day0;n;bo;hf0_d]
  t:tc[fd;genhdb_rh[day0];topic;topic;hf0;hf0_d];

  c:((>=;topic;hf0_d);(<=;topic;n));
  tf[t;c;bo;topic;hf0_d]}

bo_fdb_hdb_rdb:{[topic;hf0;day0;rf0;bo;hf0_d]
  t1:tc[fdb;rdb;topic;topic;ff0;ff0];
  t2:tc1[t1;genhdb_rh[day0];topic;topic;rf0;rf0;hf0;hf0_d];

  c:((>=;topic;hf0);(<=;topic;hf0_d));
  tf1[t2;c;bo;topic;hf0_d;rf0]}

bo_fdb_hdb_hdb:{[topic;hf0;day0;hf1;day1;bo;hf0_d;hf1_d]
  t2:tc[genhdb_rh[day0];genhdb_rh[day1];hf0;hf0_d;hf1;hf1_d];
  t:tc1[t2;rdb;hf0_d;hf0_d;hf1_d;hf1_d;topic;topic];

  c:((>=;topic;hf0_d);(<=;topic;hf1_d));
  tf1[t;c;bo;topic;hf0_d;hf1_d]}

bo_fdb_hdb_fdb:{[topic;hf0;da0;ff0;bo;hf0_d]
  t:tc1[fdb;genhdb_rh[day0];topic;topic;hf0;hf0;fh0;hf0_d];

  c:((>=;topic;hf0_d);(<=;topic;hf0));
  tf1[t;c;bo;topic;hf0;hf0_d]}

bo_fdb_fdb_num:{[topic;ff0;n;bo]
  bo_fdb_num_fdb[topic;n;ff0;bo]}

bo_fdb_fdb_rdb:{[topic;ff0;rf1;bo]
  t1:tc[fdb;fdb;topic;topic;ff0;ff0;];
  t:tc1[t1;rdb;topic;topic;ff0;ff0;rf1;rf1];
  c:((>=;topic;ff0);(<=;topic;rf1));
  tf1[t;c;bo;topic;rf1;ff0]}

bo_fdb_fdb_hdb:{[topic;ff0;hf0;day0;bo;hf0_d]
  t:tc1[fdb;genhdb_rh[day0];topic;topic;ff0;ff0;hf0;hf0_d];

  c:((>=;topic;ff0);(<=;topic;hf0_d));
  tf1:[t;c;bo;topic;ff0;hf0_d]}

bo_fdb_fdb_fdb:{[topic;ff0;ff1;bo]
  c:((>=;topic;ff0);(<=;topic;ff1));
  tf1[fdb;c;bo;topic;ff0;ff1]}

bo_hdb_num_num:{[topic1;topic;day0;n1;n2;bo]
  t:?[genhdb_rh[day0];();0b;enlist[`sym;topic]!enlist[`sym;topic1]];
  c:((>=;topic;n1);(<=;topic;n2));
  tf0[t;c;bo;topic]}

bo_hdb_num_rdb:{[topic1;topic;n;rf0;day0;bo]
  t:tc[genhdb_rh[day0];rdb;topic1;topic;rf0;rf0];

  c:((>=;topic;n);(<=;topic;rf0));
  tf[t;c;bo;topic;rf0]}

bo_hdb_num_hdb:{[topic1;topic;day0;n;topic21;topic2;day2;bo]
  t1:genhdb_rh[day0];
  t2:genhdb_rh[day2];
  t:tc[t1;t2;topic1;topic;topic21;topic2];
  c:((>=;topic;n);(<=;topic;topic2));
  tf[t;c;bo;topic;topic2]}

bo_hdb_num_fdb:{[topic1;topic;n;ff0;day0;bo]
  t:tc[genhdb_rh[day0];fdb;topic1;topic;ff0;ff0];

  c:((>=;topic;n);(<=;topic;ff0));
  tf[t;c;bo;topic;ff0]}

bo_hdb_rdb_num:{[topic1;topic;n;rf0;day0;bo]
  t:tc[genhdb_rh[day0];rdb;topic1;topic;rf0;rf0];

  c:((>=;topic;rf0);(<=;topic;n));
  tf[t;c;bo;topic;rf0]}

bo_hdb_rdb_rdb:{[topic1;topic;rf0;rf1;day0;bo]
  t1:tc[genhdb_rh[day0];rdb;topic1;topic;rf0;rf0];
  t:tc[t1;rdb;topic;topic;rf1;rf1];

  c:((>=;topic;rf0);(<=;topic;rf1));
  tf1[t;c;bo;topic;rf0;rf1]}

bo_hdb_rdb_hdb:{[topic1;topic;day0;rf0;topic21;topic2;day2;bo]
  t1:tc[genhdb_rh[day0];rdb;topic1;topic;rf0;rf0];
  t2:tc1[t1;genhdb_rh[day2];topic1;topic;rf0;rf0;topic2;topic21];
  c:((>=;topic;rf0);(<=;topic;topic2));
  tf1[t;c;bo;topic;rf0;topic2]}

bo_hdb_rdb_fdb:{[topic1;topic;rf0;ff0;day0;bo]
  t1:tc[genhdb_rh[day0];rdb;topic1;topic;rf0;rf0];
  t:tc[t1;fdb;topic;topic;ff0;ff0];

  c:((>=;topic;rf0);(<=;topic;ff0));
  tf1[t;c;bo;topic;rf0;ff0]}


bo_hdb_hdb_num:{[topic1;topic;day0;n;topic21;topic2;day2;bo]
  t1:genhdb_rh[day0];
  t2:genhdb_rh[day2];
  t:tc[t1;t2;topic1;topic;topic21;topic2];
  c:((>=;topic;topic2);(<=;topic;n));
  tf[t;c;bo;topic;topic2]}

bo_hdb_hdb_rdb:{[topic1;topic;day0;rf0;topic21;topic2;day2;bo]
  t1:tc[genhdb_rh[day0];rdb;topic1;topic;rf0;rf0];
  t2:tc1[t1;genhdb_rh[day2];topic1;topic;rf0;rf0;topic2;topic21];
  c:((>=;topic;topic2);(<=;topic;rf0));
  tf1[t2;c;bo;topic;rf0;topic2]}


/strip day
s_d:{ a:"_" vs (string x); a[0]}

bo_hdb_hdb_hdb:{[topic;day0;topic2;day2;topic3;day3;bo]
  topic1:s_d[topic];
  topic21:s_d[topic2];
  topic31:s_d[topic3];
  t1:tc[genhdb_rh[day0];genhdb_rh[day2];topic1;topic;topic21;topic2];
  t2:tc1[t1;genhdb_rh[day3];topic1;topic;topic21;topic2;topic31;topic3];
  c:((>=;topic;topic2);(<=;topic;topic3));
  tf1[t2;c;bo;topic;topic2;topic3]}

bo_hdb_hdb_fdb:{[topic1;topic;day0;ff0;topic21;topic2;day2;bo]
  t1:tc[genhdb_rh[day0];fdb;topic1;topic;ff0;ff0];
  t2:tc1[t1;genhdb_rh[day2];topic1;topic;ff0;ff0;topic2;topic21];
  c:((>=;topic;topic2);(<=;topic;ff0));
  tf1[t2;c;bo;topic;ff0;topic2]}

bo_hdb_fdb_num:{[topic1;topic;n;ff0;day0;bo]
  t:tc[genhdb_rh[day0];fdb;topic1;topic;ff0;ff0];

  c:((>=;topic;ff0);(<=;topic;n));
  tf[t;c;bo;topic;ff0]}

bo_hdb_fdb_rdb:{[topic1;topic;ff0;rf0;day0;bo]
  t1:tc[genhdb_rh[day0];rdb;topic1;topic;rf0;rf0];
  t:tc[t1;fdb;topic;topic;ff0;ff0];

  c:((>=;topic;ff0);(<=;topic;rf0));
  tf1[t;c;bo;topic;rf0;ff0]}

bo_hdb_fdb_hdb:{[topic1;topic;day0;ff0;topic21;topic2;day2;bo]
  t1:tc[genhdb_rh[day0];fdb;topic1;topic;rf0;rf0];
  t2:tc1[t1;genhdb_rh[day2];topic1;topic;ff0;ff0;topic2;topic21];
  c:((>=;topic;ff0);(<=;topic;topic2));
  tf1[t;c;bo;topic;ff0;topic2]}

bo_hdb_fdb_fdb:{[topic1;topic;ff0;ff1;day0;bo]
  t1:tc[genhdb_rh[day0];fdb;topic1;topic;ff0;ff0];
  t:tc[t1;fdb;topic;topic;ff1;ff1];

  c:((>=;topic;ff0);(<=;topic;ff1));
  tf1[t;c;bo;topic;ff0;ff1]}
