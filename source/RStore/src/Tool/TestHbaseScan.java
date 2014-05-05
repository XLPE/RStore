package Tool;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestHbaseScan {
	public static class MyMapper extends TableMapper<ImmutableBytesWritable,Put> {
		private LongWritable ts = new LongWritable(1);
	    	
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {	        	
	        	List<KeyValue> kvlist = value.getColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
	        	for(KeyValue kv: kvlist){
	        		Put put = new Put(row.copyBytes());
	        		put.add(kv);
	          		context.write(row, put);
	        	}
	   	}
	}
	
	
	

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
		// TODO Auto-generated method stub
		long start=System.currentTimeMillis();
		
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config,"scan part");
		job.setJarByClass(TestHbaseScan.class);     // class that contains mapper and reducer

		String srcTable = args[0];
		String destTable = args[1];
		
		
		
		
		DateFormat  formatter = new SimpleDateFormat("MM-dd-yyyy:HH-mm-ss");
		Date startDate;
		Date endDate;
		startDate = (Date) formatter.parse(args[2]);
		if(args[3].equals("now"))
			endDate = new Date();
		else
			endDate = (Date) formatter.parse(args[3]);
		
		
		Scan scan;
		
		
		System.out.println(startDate.toLocaleString());
		System.out.println(endDate.toLocaleString());
		scan = new Scan(startDate.getTime(), endDate.getTime());
		//scan.setMaxVersions(10);
		
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
			        
		TableMapReduceUtil.initTableMapperJob(
			"part",        // input table
			scan,
			MyMapper.class,     // mapper class
			ImmutableBytesWritable.class,         // mapper output key
			Put.class,  // mapper output value
			job);
		//job.setReducerClass(MyReducer.class);    // reducer class
		
		
		TableMapReduceUtil.initTableReducerJob(destTable, null, job);
		job.setNumReduceTasks(0);    // at least one, adjust as required
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}    
		
		long end=System.currentTimeMillis();
		long time = end -start;
		System.out.println("running time:  " + time);
	}

}
