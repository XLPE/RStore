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

public class SimulateRealtimeScan {
	
	public static class RealTimeScanMapper extends TableMapper<ImmutableBytesWritable,FloatWritable> {
		private LongWritable ts = new LongWritable(1);
	    	
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {	        	
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
		
		int numOfStores = Integer.parseInt(args[3]);
		
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config,"Simulate Real-Time Scan");
		job.setJarByClass(SimulateRealtimeScan.class);     // class that contains mapper and reducer

		Scan scan1 = new Scan(startDate.getTime(), endDate.getTime(), numOfStores);
		
		scan1.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan1.setCacheBlocks(false);  // don't set to true for MR jobs
			        
		TableMapReduceUtil.initTableMapperJob(
			"part",        // input table
			scan1,
			RealTimeScanMapper.class,     // mapper class
			ImmutableBytesWritable.class,         // mapper output key
			FloatWritable.class,  // mapper output value
			job);

		TableMapReduceUtil.initTableReducerJob(datacubeName, null, job);
		job.setNumReduceTasks(0);    // at least one, adjust as required

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
