package edu.mit.cryptdb.ssb

object Queries {
  val q1 = """
select sum(lo_extendedprice * lo_discount) as revenue
from lineorder, date_table
where
  lo_orderdate = d_datekey
  and d_year = 1993
  and lo_discount between 1 and 3
  and lo_quantity < 25;
"""

  val q2 = """
select sum(lo_revenue), d_year, p_brand1
from lineorder, date_table, part, supplier
where
  lo_orderdate = d_datekey
  and lo_partkey = p_partkey
  and lo_suppkey = s_suppkey
  and p_category = 'MFGR#12' and s_region = 'AMERICA'
group by d_year, p_brand1
order by d_year, p_brand1;
"""

  val q3 = """
select c_nation, s_nation, d_year, sum(lo_revenue) as revenue
from customer, lineorder, supplier, date_table
where
  lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_orderdate = d_datekey
  and c_region = 'ASIA'
  and s_region = 'ASIA'
  and d_year >= 1992
  and d_year <= 1997
group by c_nation, s_nation, d_year
order by d_year asc, revenue desc;
"""

  val q4 = """
select d_year, c_nation, sum(lo_revenue - lo_supplycost) as profit
from date_table, customer, supplier, part, lineorder
where
  lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_partkey = p_partkey
  and lo_orderdate = d_datekey
  and c_region = 'AMERICA'
  and s_region = 'AMERICA'
  and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by d_year, c_nation
order by d_year, c_nation
"""

  val AllQueries = Seq(q1, q2, q3, q4)

}
