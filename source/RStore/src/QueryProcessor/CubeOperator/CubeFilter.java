package QueryProcessor.CubeOperator;

import java.util.LinkedList;
import java.util.List;

import QueryProcessor.schema.Cuboid;
import QueryProcessor.schema.HBaseTable;

public class CubeFilter {
	List<String> columnList = new LinkedList<String>();
	List<String> opList = new LinkedList<String>();
	List<String> conditionList = new LinkedList<String>();

	public CubeFilter(){
		
	}
	
	public void addFilter(String column, String op, String condition){
		columnList.add(column);
		opList.add(op);
		conditionList.add(condition);
	}

	public boolean passFilter(String[] array, HBaseTable table) {
		for(int i = 0; i < columnList.size(); i ++){
			String filterColumn = columnList.get(i);
			int index = table.columnIndex.get(filterColumn);
			String columnValue = array[index];
			String op = opList.get(i);
			String condition = conditionList.get(i);
			if(!compareFunc(columnValue, op, condition))
				return false;
		}
		return true;
	}

	public boolean passFilter(String[] array, Cuboid cuboid) {
		for(int i = 0; i < columnList.size(); i ++){
			String filterColumn = columnList.get(i);
			int index = cuboid.columnIndex.get(filterColumn);
			String columnValue = array[index];
			String op = opList.get(i);
			String condition = conditionList.get(i);
			if(!compareFunc(columnValue, op, condition))
				return false;		
		}
		return true;
	}
	
	private boolean compareFunc(String columnValue, String op, String condition) {
		if(op.equals("=")){
			return columnValue.equals(condition);
		}
		else if(op.equals("<"))
			return (columnValue.compareToIgnoreCase(condition) < 0);
		else if(op.equals(">"))
			return (columnValue.compareToIgnoreCase(condition) > 0);
		else if(op.equals("<="))
			return (columnValue.compareToIgnoreCase(condition) <= 0);
		else if(op.equals(">="))
			return (columnValue.compareToIgnoreCase(condition) >= 0);
		return true;
	}

	public String serialize() {
		String result = (""+columnList.size() + "@");
		for(int i = 0; i < columnList.size(); i ++){
			result += columnList.get(i) + "," + opList.get(i) + "," + conditionList.get(i);
			if(i != columnList.size() - 1)
				result += "@";
		}
		return result;
	}

	public static CubeFilter deSerialize(String str){
		CubeFilter filter = new CubeFilter();
		String[] array = str.split("@");
		int numOfColumn = Integer.parseInt(array[0]);
		for(int i = 1; i <= numOfColumn; i ++){
			String conStr = array[i];
			String[] conArray = conStr.split(",");
			filter.columnList.add(conArray[0]);
			filter.opList.add(conArray[1]);
			filter.conditionList.add(conArray[2]);
		}
		return filter;
	}
	
    public static void main(String[] args){
    	String test = "1@@1";
    	String[] array = test.split("@");
    	System.out.println(array.length);
    	System.out.println(array[1]);
    }
}
