package tpch.constants;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * The date Writable is treated as the Text, 
 * need to re-configuration it later for a better performance
 * 
 * @author yuting
 */

final public class TPCHSchema {
	public final static Class<?> Part[] = {
	  LongWritable.class,
		Text.class,
		Text.class,
		Text.class,
		Text.class,
		LongWritable.class,
		Text.class,
		DoubleWritable.class,
		Text.class
	};
	
	public static Class<?> Supplier[] = {
		LongWritable.class, 
		Text.class, 
		Text.class,
		LongWritable.class, 
		Text.class,
		DoubleWritable.class, 
		Text.class
	};
	
	public static Class<?> Partsupp[] = {
		LongWritable.class,
		LongWritable.class,
		LongWritable.class,
		DoubleWritable.class,
		Text.class
	};

//  "c_custkey",
//  "c_name",
//  "c_address",
//  "c_nationkey",
//  "c_phone",
//  "c_acctbal",
//  "c_mktsegment",
//  "c_comment",
	public static Class<?> Customer[] = {
		LongWritable.class,
		Text.class,
		Text.class,
		LongWritable.class,
		Text.class,
		DoubleWritable.class,
		Text.class,
		Text.class
	};
	

//  "o_orderkey",
//  "o_custkey",
//  "o_orderstatus",
//  "o_totalprice",
//  "o_orderdate",
//  "o_orderpriority",
//  "o_clerk",
//  "o_shippriority",
//  "o_comment",
	public static Class<?> Orders[] = {
		LongWritable.class,
		LongWritable.class,
		Text.class,
		DoubleWritable.class,
		Text.class,				//Date
		Text.class,       //priority
		Text.class,
		LongWritable.class,
		Text.class
	};
	
	
	public static Class<?> Lineitem[] = {
		LongWritable.class,
		LongWritable.class,
		LongWritable.class,
		LongWritable.class,
		DoubleWritable.class,
		DoubleWritable.class,
		DoubleWritable.class,
		DoubleWritable.class,
		Text.class,
		Text.class,
		Text.class,				//Date
		Text.class,				//Date
		Text.class,				//Date
		Text.class,
		Text.class,
		Text.class
	};
	
	public static Class<?> Nation[] = {
		LongWritable.class, 
		Text.class, 
		LongWritable.class,
		Text.class
	};
	
	public static Class<?> Region[] = {
		LongWritable.class, 
		Text.class,
		Text.class,
	};
	
	
	public static Class<?> Q3step1[] = {
		LongWritable.class,    //orderkey
		Text.class,            //date
		LongWritable.class     //priority
	};
	
	public static Class<?> Q3step2[] = {
		LongWritable.class,        //orderkey
		DoubleWritable.class,      //extended price
		DoubleWritable.class,      //discount
		Text.class,                //date
		LongWritable.class,        //shippriroty
	};
	
	public static Class<?> Q3step1lm[] = {
		LongWritable.class,    //orderkey
		IntWritable.class,     //o_offset
	};
	public static Class<?> Q3step1test[] = {
		LongWritable.class,    //orderkey
		Text.class,            //date
		LongWritable.class,     //priority
		Text.class
	};

	public static Class<?> Q3step2test[] = {
		LongWritable.class,    //orderkey
		DoubleWritable.class,
		DoubleWritable.class,
		Text.class,            //
		Text.class,            //
		Text.class,            //
		Text.class,            //
		Text.class
	};
	
	public static Class<?>[] getTableSchema(String table){ 
		if(table.equals("lineitem")){
			return TPCHSchema.Lineitem;
		}else if(table.equals("orders")){
			return TPCHSchema.Orders;
		}else if(table.equals("customer")){
			return TPCHSchema.Customer;
		}else if(table.equals("part")){
			return TPCHSchema.Part;
		}else if(table.equals("supplier")){
			return TPCHSchema.Supplier;
		}else if(table.equals("partsupp")){
			return TPCHSchema.Partsupp; 
		}if(table.equals("nation")){
			return TPCHSchema.Nation;
		}else if(table.equals("region")){
			return TPCHSchema.Region;
		}
		else if(table.equals("q3step1")){
			return TPCHSchema.Q3step1;
		}
		else if(table.equals("q3step2")){
			return TPCHSchema.Q3step2;
		}else if(table.equals("q3step1lm")){
			return TPCHSchema.Q3step1lm;
		}else{
			System.err.println("not supported class currently: 	" + table);
			return null;
		}
	}
	
}
