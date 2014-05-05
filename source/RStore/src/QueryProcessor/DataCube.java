package QueryProcessor;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import QueryProcessor.CubeOperator.AggregationFunc;
import QueryProcessor.CubeOperator.BaselineOperator;
import QueryProcessor.CubeOperator.CostModel;
import QueryProcessor.CubeOperator.CubeFilter;
import QueryProcessor.CubeOperator.GroupBy;
import QueryProcessor.CubeOperator.IncreQueryOperator;
import QueryProcessor.CubeOperator.SimpleCostModel;
import QueryProcessor.schema.Cuboid;
import QueryProcessor.schema.HBaseTable;
import QueryProcessor.schema.Schema;



public class DataCube {	
	public String tableName;
	public HBaseTable table;
	public Cuboid cuboid;
	public CubeFilter cubeFilter = new CubeFilter();
	public GroupBy groupBy = new GroupBy();
	public AggregationFunc aggregateFunc = new AggregationFunc();
	
	public String outputPath;
	public long queryingTime;
	public long cubeRefreshTime;
	public Path cubePath;
	public double updateRatio = 1;
	
	DataCube(String tableName, String schemaPath){
		this.tableName = tableName;
		Schema schema = new Schema(schemaPath);
		table = schema.getTable(tableName);
		cuboid = schema.getCube(tableName);
	}
	
	public DataCube(){		
	}
	
    public void loadSchema(String schemaPath){
    	
    }
    public void addFilter(String columnName, String op, String condition){
    	cubeFilter.addFilter(columnName, op, condition);
    }
    
    public void addGroupBy(String column){
    	groupBy.add(column);
    }
    
    public void setAggregationFunc(String funcName){
    	aggregateFunc.setFunction(funcName);
    }
    public void setOutputPath(String path){
    	outputPath = path;
    }
    
    public boolean checkOperator(){
    	//TODO
    	return true;
    }
	public void execute(){
    	if(checkOperator()){
    		
    	}
    	setTimeStamp();
    	CostModel model = new SimpleCostModel(this);
    	if(model.isIncreQueryingBetter()){
    		IncreQueryExe();
    	}
    	else{
    		BaselineExe();
    	}
    }

	private void setTimeStamp() {
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(conf);
			String dir = "/cube";
			FileStatus[] files = fs.listStatus(new Path(dir));
			cubePath = files[files.length - 1].getPath();
			String dateStr = cubePath.getName();
			try {
				this.cubeRefreshTime = ROLAPUtil.getTimestamp(dateStr);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.queryingTime = System.currentTimeMillis();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void BaselineExe() {		
		try {
			BaselineOperator.exe(this);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	private void IncreQueryExe() {
		// TODO Auto-generated method stub
		
	}
	
	public String serialize(){
		String result = "";
		result += tableName + "#";
		result += table.serialize() + "#";
		result += cuboid.serialize() + "#";
		result += cubeFilter.serialize() + "#";
		result += groupBy.serialize();
		
		return result;
	}
	public static DataCube deSerialize(String datacubestr){
		DataCube dc = new DataCube();
		System.out.println(datacubestr);
		String[] array = datacubestr.split("#");
		dc.tableName = array[0];
		dc.table = HBaseTable.deSerialize(array[1]);
		dc.cuboid = Cuboid.deSerialize(array[2]);
		dc.cubeFilter = CubeFilter.deSerialize(array[3]);
		dc.groupBy = GroupBy.deSerialize(array[4]);
		return dc;
	}
	
	public static void main(String[] args){
		DataCube dc = new DataCube("part", "schema");		
		dc.addFilter("p_type", "=", "asdf");
		dc.addFilter("p_container", "=", "asdf2");
		dc.addGroupBy("p_brand");
		String dcStr = dc.serialize();
		DataCube dc2 = dc.deSerialize(dcStr);
		System.out.println(dc.serialize());
		System.out.println(dc2.serialize());
		
		long t = Long.parseLong("1399188180000");
		System.out.println(t);
	    String str = ROLAPUtil.getTimeStr(t);
		System.out.println(str);
		try {
			System.out.println(ROLAPUtil.getTimestamp(str));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
