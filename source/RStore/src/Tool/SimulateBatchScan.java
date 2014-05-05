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

public class SimulateBatchScan {
	
	public static class BatchScanMapper extends TableMapper<ImmutableBytesWritable,FloatWritable> {
		
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {	        	
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

		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config,"Simulate Batch Scan");
		job.setJarByClass(SimulateBatchScan.class);     // class that contains mapper and reducer
			        
		Scan scan = new Scan();
		scan.setTimeRange(0, date.getTime());
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		// set other scan attrs
			        
		TableMapReduceUtil.initTableMapperJob(
			"part",        // input table
			scan,               // Scan instance to control CF and attribute selection
			BatchScanMapper.class,     // mapper class
			ImmutableBytesWritable.class,         // mapper output key
			FloatWritable.class,  // mapper output value
			job);
		TableMapReduceUtil.initTableReducerJob(datacubeName, null, job);
		job.setNumReduceTasks(0);    // at least one, adjust as required
			    
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
