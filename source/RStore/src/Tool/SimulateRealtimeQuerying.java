package Tool;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Tool.BuildFullCube.BuildingCubeReducer;
import Tool.ScanTwoTable.MyMapper;
import Tool.ScanTwoTable.MyReducer;

public class SimulateRealtimeQuerying {
	
	public static class QueryMapper extends TableMapper<ImmutableBytesWritable,FloatWritable> {
		private LongWritable ts = new LongWritable(1);
	    	
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {	        	
	        	List<KeyValue> kvlist = value.getColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
	        	int i = 0;
	        	String dimensionAttr = null;
	        	float value1 = 0;
	        	float value2;
	        	for(KeyValue kv: kvlist){
	        		Map<String, Object> smap = kv.toStringMap();
	        		String valueStr = Bytes.toString(kv.getValue());
	        		String array[] = valueStr.split("\\|");
	        		if(array.length < 6){
	        			String rowStr = Bytes.toString(row.copyBytes());
	        			String rowArray[] = rowStr.split("\\|");
	        			if(!rowArray[0].equals("Manufacturer#1"))
    	        			continue;
	        			dimensionAttr = rowArray[1] + "|" + rowArray[2] + "|" + rowArray[3] + "|" + rowArray[4];
	        			context.write(new ImmutableBytesWritable(Bytes.toBytes(dimensionAttr)), new FloatWritable(Float.parseFloat(array[0])));
	        		}
	        		else{
	        			if(!array[1].equals("Manufacturer#1"))
    	        			continue;
	        			if(dimensionAttr == null){
	    	        		dimensionAttr = array[2] + "|" + array[3] + "|" + array[4] + "|" + array[5];
	    	        		value1 = Float.parseFloat(array[6]);
	    	        		context.write(new ImmutableBytesWritable(Bytes.toBytes(dimensionAttr)), new FloatWritable(value1));
	        			}
	        			else{
	        				value2 = Float.parseFloat(array[6]);
	        				context.write(new ImmutableBytesWritable(Bytes.toBytes(dimensionAttr)), new FloatWritable(0 - value2));
	        			}
	        		}
	        	}
	   	}
	}
	
	
	public static class QueryReducer extends TableReducer<ImmutableBytesWritable, FloatWritable, ImmutableBytesWritable>  {
	 	public void reduce(ImmutableBytesWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
	    		float i = 0;
	    		for (FloatWritable val : values) {
	    			i += val.get();
	    		}
	    		Put put = new Put(key.copyBytes());
	    		put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(Float.toString(i)));
	    		context.write(null, put);
	   	}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
		long start = System.currentTimeMillis();
		
		DateFormat  formatter = new SimpleDateFormat("MM-dd-yyyy:HH-mm-ss");
		Date startDate;
		Date endDate;
		startDate = (Date) formatter.parse(args[0]);
		if(args[1].equals("now"))
			endDate = new Date();
		else
			endDate = (Date) formatter.parse(args[1]);
		
		String datacubeName = args[2];
		int numOfReducers = Integer.parseInt(args[3]);
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config,"Simulate Real-Time querying");
		job.setJarByClass(ScanTwoTable.class);     // class that contains mapper and reducer

		Scan scan1 = new Scan(startDate.getTime(), endDate.getTime());
		
		scan1.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan1.setCacheBlocks(false);  // don't set to true for MR jobs
			        
		TableMapReduceUtil.initTableMapperJob(
			"part",        // input table
			scan1,
			QueryMapper.class,     // mapper class
			ImmutableBytesWritable.class,         // mapper output key
			FloatWritable.class,  // mapper output value
			job);
		TableMapReduceUtil.initTableReducerJob(
				datacubeName,        // output table
				QueryReducer.class,    // reducer class
				job);
		job.setNumReduceTasks(numOfReducers);    // at least one, adjust as required
	

		TableMapReduceUtil.addDependencyJars(job);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}    
		
		long end=System.currentTimeMillis();
		long time = end - start;
		System.out.println("running time:  " + time);
		
		Date date = new Date();
		System.out.println("start: " + startDate.toLocaleString() + "  timestamp:  " + startDate.getTime());
		System.out.println("data cube refreshing time:" + endDate.toLocaleString() + "  timestamp:  " + endDate.getTime());
		System.out.println( "data cube refreshed time: " + date.toLocaleString() + "  timestamp:  " + date.getTime());
	}
}
