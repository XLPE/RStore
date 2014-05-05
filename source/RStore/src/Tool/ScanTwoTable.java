package Tool;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mymapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScanTwoTable {
	public static class MyMapper extends TableMapper<Text,Text> {
		private LongWritable ts = new LongWritable(1);
	    	
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {	        	
	        	List<KeyValue> kvlist = value.getColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
	        	for(KeyValue kv: kvlist){
	        		Map<String, Object> smap = kv.toStringMap();
	          		context.write(new Text((String)smap.get("row")), new Text(kv.getValue()));
	        	}
	   	}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, Context context)
				throws IOException, InterruptedException{
			while (values.hasNext()) {
				context.write(key, values.next());
			}
		}
	}
	
	
	

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		long start=System.currentTimeMillis();
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config,"GetTimestamps");
		job.setJarByClass(ScanTwoTable.class);     // class that contains mapper and reducer

		Scan scan1 = new Scan();
		Scan scan2 = new Scan();
		
		scan1.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan1.setCacheBlocks(false);  // don't set to true for MR jobs
		scan2.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan2.setCacheBlocks(false);  // don't set to true for MR jobs
			        
		TableMapReduceUtil.initTableMapperJob(
			"customer",        // input table
			"part",
			scan1,
			scan2,
			MyMapper.class,     // mapper class
			Text.class,         // mapper output key
			Text.class,  // mapper output value
			job);
		job.setReducerClass(MyReducer.class);    // reducer class
		job.setNumReduceTasks(2);    // at least one, adjust as required
	
		FileOutputFormat.setOutputPath(job, new Path("/tmp"));  // adjust directories as required

		TableMapReduceUtil.addDependencyJars(job);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}    
		
		long end=System.currentTimeMillis();
		long time = end -start;
		System.out.println("running time:  " + time);
	}

}
