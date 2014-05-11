package tpch.constants;

/**
 * The column name
 * 
 * @author yuting
 */
final public class TPCHColumnName {
	public final static String Part[] = { "p_partkey", "p_name", "p_mfgr",
			"p_brand", "p_type", "p_size", "p_container", "p_retailprice",
			"p_comment", };

	public static String Supplier[] = { "s_suppkey", "s_name", "s_address",
			"s_nationkey", "s_phone", "s_addtbal", "s_comment", };

	public static String Partsupp[] = { "ps_partkey", "ps_suppkey",
			"ps_availqty", "ps_supplycost", "ps_comment", };

	public static String Customer[] = { "c_custkey", "c_name", "c_address",
			"c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment", };

	public static String Orders[] = { "o_orderkey", "o_custkey",
			"o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority",
			"o_clerk", "o_shippriority", "o_comment", };

	public static String Lineitem[] = { "l_orderkey", "l_partkey", "l_suppkey",
			"l_linenumber", "l_quantity", "l_extendedprice", "l_discount",
			"l_tax", "l_returnflag", "l_linestatus", "l_shipdate",
			"l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode",
			"l_comment", };

	public static String Nation[] = { "n_nationkey", "n_name", "n_regionkey",
			"n_comment", };

	public static String Region[] = { "r_regionkey", "r_name", "r_comment", };

	public static String Q3step1[] = { "o_orderkey", "o_orderdate",
			"o_shippriority", };
	public static String Q3step2[] = { "l_orderkey", "l_extendedprice", "l_discount", 
		"o_orderdate",  "o_shippriority", };
	public static String Q3step1lm[] = { "o_orderkey", "o_offset" };
	public static String Q3step1test[] = { "o_orderkey", "o_orderdate",
		"o_shippriority", "o_orderpriority"};
	public static String Q3step2test[] = { "l_orderkey", "l_extendedprice",
		"l_discount", "l_shipinstruct", "l_shipmode", "o_orderdate", "o_shippriority", "o_orderpriority"};
}
