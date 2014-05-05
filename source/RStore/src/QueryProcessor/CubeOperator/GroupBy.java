package QueryProcessor.CubeOperator;

import java.util.LinkedList;
import java.util.List;

import QueryProcessor.schema.Cuboid;
import QueryProcessor.schema.HBaseTable;

public class GroupBy {
	List<String> groupedColumns = new LinkedList<String>();

	public void add(String column) {
		groupedColumns.add(column);
	}

	public String getGroupKey(String[] array, HBaseTable table) {
		// using the same key for all the tuples and cubes.
		if (groupedColumns.size() == 0)
			return "1";
		String groupKey = "";
		for (int i = 0; i < groupedColumns.size(); i++) {
			String column = groupedColumns.get(i);
			int index = table.columnIndex.get(column);
			groupKey += array[index];
			if(i != groupedColumns.size() - 1)
				groupKey += "|";
		}
		return groupKey;
	}

	public String getGroupKey(String[] array, Cuboid cuboid) {
		// using the same key for all the tuples and cubes.
		if (groupedColumns.size() == 0)
			return "1";
		String groupKey = "";
		for (int i = 0; i < groupedColumns.size(); i++) {
			String column = groupedColumns.get(i);
			int index = cuboid.columnIndex.get(column);
			groupKey += array[index];
			if(i != groupedColumns.size() - 1)
				groupKey += "|";
		}
		return groupKey;
	}

	public String serialize() {
		String result = "" + groupedColumns.size() + "@";
		int i = 0;
		for(String c : groupedColumns){
			if(i == groupedColumns.size() - 1)
			    result += c;
			else
				result += c + "@";
			i ++;
		}		
		return result;
	}

	public static GroupBy deSerialize(String str) {
		String[] array = str.split("@");
		GroupBy gb = new GroupBy();
		int numOfCol = Integer.parseInt(array[0]);
		for(int i = 1; i <= numOfCol; i ++){
			gb.groupedColumns.add(array[i]);
		}
		return gb;
	}
}
