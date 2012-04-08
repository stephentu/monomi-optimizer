import org.specs2.mutable._

class SQLParserSpec extends Specification {

  "SQLParser" should {
    "parse query1" in {
      val parser = new SQLParser
      val r = parser.parse("""
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from
  lineitem
where
  l_shipdate <= date '1998-12-01' - interval '5' day
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;""")

      r should beSome
    }

    "parse query2" in {
      val parser = new SQLParser
      val r = parser.parse("""
select
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
from
  part,
  supplier,
  partsupp,
  nation,
  region
where
  p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 10
  and p_type like '%foo'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'somename'
  and ps_supplycost = (
    select
      min(ps_supplycost)
    from
      partsupp,
      supplier,
      nation,
      region
    where
      p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'somename'
  )
order by
  s_acctbal desc,
  n_name,
  s_name,
  p_partkey
limit 100;
""")    
      r should beSome
    }

    "parse query3" in {
      val parser = new SQLParser
      val r = parser.parse(
"""
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'somesegment'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1999-01-01'
	and l_shipdate > date '1999-01-01'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10;
""")
      r should beSome
    }

    "parse query4" in {
      val parser = new SQLParser
      val r = parser.parse(
"""
select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= date '1999-01-01'
	and o_orderdate < date '1999-01-01' + interval '3' month
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority;
""")
      r should beSome
    }

    "parse query5" in {
      val parser = new SQLParser
      val r = parser.parse(
"""
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'foo'
	and o_orderdate >= date '1999-01-01'
	and o_orderdate < date '1999-01-01' + interval '1' year
group by
	n_name
order by
	revenue desc;
""")
      r should beSome
    }

    "parse query6" in {
      val parser = new SQLParser
      val r = parser.parse(
"""
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1999-01-01'
	and l_shipdate < date '1999-01-01' + interval '1' year
	and l_discount between 2 - 0.01 and 2 + 0.01
	and l_quantity < 3;
""")
      r should beSome
    }

    "parse query7" in {
      val parser = new SQLParser
      val r = parser.parse(
"""
select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = 'a' and n2.n_name = 'b')
				or (n1.n_name = 'b' and n2.n_name = 'a')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
""")
      r should beSome
    }
  }
}
