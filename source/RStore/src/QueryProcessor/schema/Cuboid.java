package QueryProcessor.schema;

import java.util.HashMap;

public class Cuboid {
	public HashMap<String, Integer> columnIndex = new HashMap<String,Integer>();
	public HashMap<Integer, String> indexToColumn = new HashMap<Integer, String>();
	public String numericalColumn;
	public int numOfDimensionCol = 0;
	public String cuboidName;
	
	Cuboid(){		
	}
	Cuboid(String cuboidName){
		this.cuboidName = cuboidName;
	}
		
	public void addColumn(String name, boolean isNumericalColumn){
		Column column = new Column(name, 0);
		if(isNumericalColumn)
			numericalColumn = name;
		else{
			columnIndex.put(name, numOfDimensionCol);
			indexToColumn.put(numOfDimensionCol, name);
			numOfDimensionCol ++;
		}
	}
	public String serialize() {
		String result = numericalColumn + "@";
		for(int i = 0; i < numOfDimensionCol; i ++){
			if(i == numOfDimensionCol - 1)
				result += indexToColumn.get(i);
			else
				result += indexToColumn.get(i) + ",";
		}
		result += "@" + cuboidName;
		return result;
	}
	
	public static Cuboid deSerialize(String str){
		Cuboid temp = new Cuboid();
		String[] array = str.split("@");
		temp.addColumn(array[0], true);
		String[] columns = array[1].split(",");
		for(int i = 0; i < columns.length; i ++){
			temp.addColumn(columns[i], false);
		}
		temp.cuboidName = array[2];
		return temp;
	}
	
}
