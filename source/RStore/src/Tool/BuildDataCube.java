package Tool;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class BuildDataCube {
	
	public static class BuildingCubeMapper extends TableMapper<ImmutableBytesWritable,FloatWritable> {
		
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {	        	
	        	List<KeyValue> kvlist = value.getColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
	        	for(KeyValue kv: kvlist){
	        		String valueStr = Bytes.toString(kv.getValue());
	        		String array[] = valueStr.split("\\|");
	        		String dimensionAttr = array[1] + "|" + array[2] + "|" + array[3] + "|" + array[4] + "|" + array[5];
	        		context.write(new ImmutableBytesWritable(Bytes.toBytes(dimensionAttr)), new FloatWritable(Float.parseFloat(array[6])) );
	        	}
	   	}
	}
	
	public static class BuildingCubeReducer extends TableReducer<ImmutableBytesWritable, FloatWritable, ImmutableBytesWritable>  {
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
	    
	public static void main(String args[]) throws ParseException, IOException, InterruptedException, ClassNotFoundException{
		long start = System.currentTimeMillis();
		String datacubeName = args[1];
		DateFormat  formatter = new SimpleDateFormat("MM-dd-yyyy:HH-mm-ss");
		Date date;
		if(args[0].equals("now"))
			date = new Date();
		else
			date = (Date) formatter.parse(args[0]);

		int numOfReducers = Integer.parseInt(args[2]);
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config,"Build Data Cube");
		job.setJarByClass(BuildDataCube.class);     // class that contains mapper and reducer
			        
		Scan scan = new Scan();
		scan.setTimeRange(0, date.getTime());
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		// set other scan attrs
			        
		TableMapReduceUtil.initTableMapperJob(
			"part",        // input table
			scan,               // Scan instance to control CF and attribute selection
			BuildingCubeMapper.class,     // mapper class
			ImmutableBytesWritable.class,         // mapper output key
			FloatWritable.class,  // mapper output value
			job);
		TableMapReduceUtil.initTableReducerJob(
			datacubeName,        // output table
			BuildingCubeReducer.class,    // reducer class
			job);
		job.setNumReduceTasks(numOfReducers);   // at least one, adjust as required
			    
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}    
		
		long end=System.currentTimeMillis();
		long time = end - start;
		
		System.out.println("running time:  " + time);
		
		System.out.println("datacube building time: " + date.toLocaleString() + "  timestamp:  " + date.getTime());
		date = new Date();
		System.out.println("datacube built time: " + date.toLocaleString() + "  timestamp:  " + date.getTime());
		
	}
}
