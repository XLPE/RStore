package QueryProcessor.schema;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.HashMap;

import com.google.common.collect.Table;

public class Schema {
	public HashMap<String, HBaseTable> tables = new HashMap<String, HBaseTable>();
	public HashMap<String, Cuboid> cuboids = new HashMap<String, Cuboid>();

	public void addTable(HBaseTable table) {
		tables.put(table.tableName, table);
	}

	public Schema(String schemaFilePath) {
		this.initialSchema(schemaFilePath);
	}

	public void initialSchema(String schemaFilePath) {
		// initial schemas
		try {
			RandomAccessFile input = new RandomAccessFile(new File(
					schemaFilePath), "r");
			String line;
			while ((line = input.readLine()) != null) {
				//load table schema
				String[] array = line.split(",");
				String tableName = array[0];
				double tableSize = Integer.parseInt(array[1]);
				HBaseTable table = null;
				table = new HBaseTable(tableName, tableSize);
				line = input.readLine();
				array = line.split(",");
				int n = array.length / 3;
				for (int j = 0; j < n; j++) {
					String columnName = array[j * 3];
					String sizeStr = array[j * 3 + 1];
					String isKeyStr = array[j * 3 + 2];
					float size = Float.parseFloat(sizeStr);
					boolean isKey = (isKeyStr.equals("1"));
					table.addColumn(columnName, size, isKey);
				}
				this.tables.put(tableName, table);
				
                //load cube schema
				Cuboid cuboid = new Cuboid(tableName + "cube");
				line = input.readLine();
				array = line.split(",");
				for (int j = 0; j < array.length; j++) {
					if (j == array.length - 1) {
						cuboid.addColumn(array[j], true);
					} else {
						cuboid.addColumn(array[j], false);
					}
				}
				this.cuboids.put(tableName, cuboid);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		Schema schema = new Schema("schema");
	}

	public HBaseTable getTable(String tableName) {
		return tables.get(tableName);
	}

	public Cuboid getCube(String tableName) {
		return cuboids.get(tableName);
	}
	
	public String toString(){
		return this.tables.size() + "";
	}
}
