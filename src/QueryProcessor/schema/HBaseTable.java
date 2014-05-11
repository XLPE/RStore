package QueryProcessor.schema;
import java.util.HashMap;


public class HBaseTable {
	public HashMap<String, Column> columns = new HashMap<String, Column>();
	public HashMap<String, Integer> columnIndex = new HashMap<String,Integer>();
	public HashMap<Integer, String> indexToColumn = new HashMap<Integer, String>();
	public Column key;
	public int numOfColumn = 0;
	
	public double numOfTuples;
	public float tupleSize = 0;
	public String tableName;
	
	HBaseTable(){
		
	}
	HBaseTable(String name, double tableSize){
		tableName = name;
		this.numOfTuples = tableSize;
	}
		
	public void addColumn(String name, float size, boolean isKey){
		Column column = new Column(name, size);		
		if(isKey)
			key = column;
		else{
			columns.put(name, column);
			columnIndex.put(name, numOfColumn);
			indexToColumn.put(numOfColumn, name);
			numOfColumn ++;
		}
		tupleSize += size;
	}

	public String serialize() {
		String result = key.columnName + "@";
		for(int i = 0; i < numOfColumn; i ++){
			if(i == numOfColumn - 1)
				result += indexToColumn.get(i);
			else
				result += indexToColumn.get(i) + ",";
		}
		result += "@" + tableName;
		return result;
	}
	
	public static HBaseTable deSerialize(String str){
		HBaseTable temp = new HBaseTable();
		String[] array = str.split("@");
		temp.addColumn(array[0], 0, true);
		String[] columns = array[1].split(",");
		for(int i = 0; i < columns.length; i ++){
			temp.addColumn(columns[i], 0, false);
		}
		temp.tableName = array[2];
		return temp;
	}
	
}
