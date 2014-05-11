package tpch.constants;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class TPCHConstant {
	public final static String TableName = "tpch.table.name";
	public final static String RowTableDir = "row.table.dir";
	public final static String ColTableDir = "col.table.dir";
	public final static String ZebraColTableDir = "zebra.table.dir";
	public final static String KeyColumnID = "key.column.id";
	public final static String ColumnRecordPerBlock = "column.record.per.block";
	public final static String TPCHScale = "tpch.scale";
	public final static String PartitionRange = "column.partition.range";
	public final static String TableUnit = "table.unit.scale";
	public final static String CompressClass = "column.compress.class";
	public final static String WriteResult = "write.hdfs";
	public final static String SelectTimeThreadhold = "thread.time.string";
	public static int blocksize = 64;
	public final static int defaultRecordPerBlock = 1 << 12;

	public static void setTPCHScale(int scale, Configuration conf) {
		conf.setInt(TPCHScale, scale);
	}

	public static int getTPCHScale(Configuration conf) {
		return conf.getInt(TPCHScale, 0);
	}

	@Deprecated
	public static Class<?>[] getTableSchema(Configuration job) {
		String table = job.get(TPCHConstant.TableName);
		return getTableSchema(table);
	}

	public static Class<?>[] getTableSchema(String table) {
		Class<?> schemaClass[] = null;
		if (table.equals("lineitem")) {
			schemaClass = TPCHSchema.Lineitem;
		} else if (table.equals("orders")) {
			schemaClass = TPCHSchema.Orders;
		} else if (table.equals("customer")) {
			schemaClass = TPCHSchema.Customer;
		} else if (table.equals("part")) {
			schemaClass = TPCHSchema.Part;
		} else if (table.equals("supplier")) {
			schemaClass = TPCHSchema.Supplier;
		} else if (table.equals("partsupp")) {
			schemaClass = TPCHSchema.Partsupp;
		} else if (table.equals("nation")) {
			schemaClass = TPCHSchema.Nation;
		} else if (table.equals("region")) {
			schemaClass = TPCHSchema.Region;
		} else if (table.equals("q3step1")) {
			schemaClass = TPCHSchema.Q3step1;
		} else if(table.equals("q3step2")){
			return TPCHSchema.Q3step2;
		}else if(table.equals("q3step1lm")){
			return TPCHSchema.Q3step1lm;
		}else if (table.equals("q3step1test")) {
			schemaClass = TPCHSchema.Q3step1test;
		}else if (table.equals("q3step2test")) {
			schemaClass = TPCHSchema.Q3step2test;
		}else {
			System.err.println("not supported class currently:  " + table);
			System.exit(-1);
		}
		return schemaClass;
	}

	public static String[] getTableColumnsName(String table) {
		if (table.equals("lineitem")) {
			return TPCHColumnName.Lineitem;
		} else if (table.equals("orders")) {
			return TPCHColumnName.Orders;
		} else if (table.equals("customer")) {
			return TPCHColumnName.Customer;
		} else if (table.equals("part")) {
			return TPCHColumnName.Part;
		} else if (table.equals("supplier")) {
			return TPCHColumnName.Supplier;
		} else if (table.equals("partsupp")) {
			return TPCHColumnName.Partsupp;
		}
		if (table.equals("nation")) {
			return TPCHColumnName.Nation;
		} else if (table.equals("region")) {
			return TPCHColumnName.Region;
		} else if (table.equals("q3step1")) {
			return TPCHColumnName.Q3step1;
		} else if (table.equals("q3step2")) {
			return TPCHColumnName.Q3step2;
		} else if (table.equals("q3step1lm")) {
			return TPCHColumnName.Q3step1lm;
		}  else if (table.equals("q3step1test")) {
			return TPCHColumnName.Q3step1test;
		}else if (table.equals("q3step2test")) {
			return TPCHColumnName.Q3step2test;
		}else {
			System.err.println("not supported class currently:  " + table);
			return null;
		}
	}

	public static String fullName(String type) throws IOException {
		if ("L".equals(type)) {
			return "lineitem";
		} else if ("O".equals(type)) {
			return "orders";
		} else if ("S".equals(type)) {
			return "partsupp";
		} else if ("c".equals(type)) {
			return "customer";
		} else if ("P".equals(type)) {
			return "part";
		} else if ("s".equals(type)) {
			return "supplier";
		} else if ("n".equals(type)) {
			return "nation";
		} else if ("r".equals(type)) {
			return "region";
		}
		throw new IOException("unknown abbreviation: " + type);
	}

	public static String abbreviation(String table) throws IOException {
		if ("lineitem".equals(table))
			return "L";
		else if ("orders".equals(table))
			return "O";
		else if ("partsupp".equals(table))
			return "S";
		else if ("customer".equals(table))
			return "c";
		else if ("part".equals(table))
			return "P";
		else if ("supplier".equals(table))
			return "s";
		else if ("nation".equals(table))
			return "n";
		else if ("region".equals(table))
			return "r";
		else
			throw new IOException("unknown table name: " + table);
	}

	public static String getRowDir(String table, String userName, int scale) {
		return "/tpch" + scale + "_" + userName +  "/" + table;
	}

	public static String getColumnDir(String table, int scale) {
		return "/col" + scale + "/" + table;
	}

	public static String getBitmapDir(String table, int scale, String Column) {
		return "/bitmap" + scale + "/" + table + "/" + Column;
	}

	/**
	 * 
	 * @param table
	 * @param scale
	 * @param keyColumn
	 *            It is the sorted column in the Group
	 * @return
	 */
	public static String getPFDir(String table, int scale, String keyColumn) {
		return "/col" + scale + "/" + table + "/PF." + keyColumn;
	}

	public static int getColumnID(String columnName, String columns[]) {
		for (int i = 0; i < columns.length; i++) {
			if (columns[i].equalsIgnoreCase(columnName))
				return i;
		}
		return -1;
	}

	public static int calculateSplits(int scale, String type){
		int ret = -1;
		if ("L".equals(type)) {
			// return "lineitem";
			ret = Math.max(2, scale);
		} else if ("O".equals(type)) {
			// return "orders";
			ret = Math.max(2, scale / 3);
		} else if ("S".equals(type)) {
			// return "partsupp";
			ret = Math.max(2, scale / 5);
		} else if ("c".equals(type)) {
			// return "customer";
			ret = Math.max(2, scale / 20);
		} else if ("P".equals(type)) {
			// return "part";
			ret = Math.max(2, scale / 20);
		} else if ("s".equals(type)) {
			// return "supplier";
			ret = Math.max(2, scale / 200);
		} else if ("n".equals(type)) {
			// return "nation";
			ret = 1;
		} else if ("r".equals(type)) {
			// return "region";
			ret = 1;
		} 
		
		if (ret < 1)
			ret = 1;
	
		return ret;
		/*
		if ("L".equals(type)) {
			// return "lineitem";
			ret = Math.max(1, (int)(scale * 724.67/blocksize));
			//ret = Math.max(2, (int)(scale * 730/blocksize) + 1);
		} else if ("O".equals(type)) {
			// return "orders";
			ret = Math.max(1, (int)(scale * 163.98/blocksize));
			//ret = Math.max(2, (int)(scale * 165/blocksize) + 1);
		} else if ("S".equals(type)) {
			// return "partsupp";
			ret = Math.max(1,(int)(scale * 113.47/blocksize));
			//ret = Math.max(2,(int)(scale * 115/blocksize) + 1);
		} else if ("c".equals(type)) {
			// return "customer";
			ret = Math.max(1,(int)(scale * 23.22/blocksize));
		} else if ("P".equals(type)) {
			// return "part";
			ret = Math.max(1,(int)(scale * 23.09/blocksize));
		} else if ("s".equals(type)) {
			// return "supplier";
			ret = Math.max(1,(int)(scale * 1.37/blocksize));
		} else if ("n".equals(type)) {
			// return "nation";
			ret = 1;
		} else if ("r".equals(type)) {
			// return "region";
			ret = 1;
		} else
			throw new IOException("unknown abbreviation: " + type);
		if (ret < 1)
			ret = 1;
		return ret;
		*/
	}

	public static int getRecordsPerBlock(Configuration conf) {
		return conf.getInt(TPCHConstant.ColumnRecordPerBlock,
				defaultRecordPerBlock);
	}

}
