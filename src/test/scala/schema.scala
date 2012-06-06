package edu.mit.cryptdb

// this relies on having a valid connection to the database
trait SchemaInit {
  import util.{ Properties => ScalaProperties }
  def init(): Schema = {
    val _props = new java.util.Properties
    _props.setProperty("user", ScalaProperties.envOrElse("DB_USER", "stephentu"))
    // TODO: password for properties?

    new PgSchema(
      ScalaProperties.envOrElse("DB_HOSTNAME", "localhost"), 
      ScalaProperties.envOrElse("DB_PORT", "5432").toInt, 
      ScalaProperties.envOrElse("DB_DATABASE", "tpch_0_05"),
      _props)
  }
}

object TestSchema {
  val definition = new Definitions(Map("nation" -> Seq(TableColumn("n_nationkey", IntType(4), true), TableColumn("n_name", FixedLenString(25), false), TableColumn("n_regionkey", IntType(4), false), TableColumn("n_comment", VariableLenString(152), false)), "partsupp" -> Seq(TableColumn("ps_partkey", IntType(4), true), TableColumn("ps_suppkey", IntType(4), true), TableColumn("ps_availqty", IntType(4), false), TableColumn("ps_supplycost", DecimalType(15,2), false), TableColumn("ps_comment", VariableLenString(199), false)), "supplier" -> Seq(TableColumn("s_suppkey", IntType(4), true), TableColumn("s_name", FixedLenString(25), false), TableColumn("s_address", VariableLenString(40), false), TableColumn("s_nationkey", IntType(4), false), TableColumn("s_phone", FixedLenString(15), false), TableColumn("s_acctbal", DecimalType(15,2), false), TableColumn("s_comment", VariableLenString(101), false)), "customer" -> Seq(TableColumn("c_custkey", IntType(4), true), TableColumn("c_name", VariableLenString(25), false), TableColumn("c_address", VariableLenString(40), false), TableColumn("c_nationkey", IntType(4), false), TableColumn("c_phone", FixedLenString(15), false), TableColumn("c_acctbal", DecimalType(15,2), false), TableColumn("c_mktsegment", FixedLenString(10), false), TableColumn("c_comment", VariableLenString(117), false)), "region" -> Seq(TableColumn("r_regionkey", IntType(4), true), TableColumn("r_name", FixedLenString(25), false), TableColumn("r_comment", VariableLenString(152), false)), "orders" -> Seq(TableColumn("o_orderkey", IntType(4), true), TableColumn("o_custkey", IntType(4), false), TableColumn("o_orderstatus", FixedLenString(1), false), TableColumn("o_totalprice", DecimalType(15,2), false), TableColumn("o_orderdate", DateType, false), TableColumn("o_orderpriority", FixedLenString(15), false), TableColumn("o_clerk", FixedLenString(15), false), TableColumn("o_shippriority", IntType(4), false), TableColumn("o_comment", VariableLenString(79), false)), "lineitem" -> Seq(TableColumn("l_orderkey", IntType(4), true), TableColumn("l_partkey", IntType(4), false), TableColumn("l_suppkey", IntType(4), false), TableColumn("l_linenumber", IntType(4), true), TableColumn("l_quantity", DecimalType(15,2), false), TableColumn("l_extendedprice", DecimalType(15,2), false), TableColumn("l_discount", DecimalType(15,2), false), TableColumn("l_tax", DecimalType(15,2), false), TableColumn("l_returnflag", FixedLenString(1), false), TableColumn("l_linestatus", FixedLenString(1), false), TableColumn("l_shipdate", DateType, false), TableColumn("l_commitdate", DateType, false), TableColumn("l_receiptdate", DateType, false), TableColumn("l_shipinstruct", FixedLenString(25), false), TableColumn("l_shipmode", FixedLenString(10), false), TableColumn("l_comment", VariableLenString(44), false)), "part" -> Seq(TableColumn("p_partkey", IntType(4), true), TableColumn("p_name", VariableLenString(55), false), TableColumn("p_mfgr", FixedLenString(25), false), TableColumn("p_brand", FixedLenString(10), false), TableColumn("p_type", VariableLenString(25), false), TableColumn("p_size", IntType(4), false), TableColumn("p_container", FixedLenString(10), false), TableColumn("p_retailprice", DecimalType(15,2), false), TableColumn("p_comment", VariableLenString(23), false))), None)
}
