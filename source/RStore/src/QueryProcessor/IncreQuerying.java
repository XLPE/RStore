package QueryProcessor;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class IncreQuerying {
	
	public static class RealTimeScanMapper extends TableMapper<Text,FloatWritable> {
		private LongWritable ts = new LongWritable(1);
		private Text cubeKey = new Text("0");
		private FloatWritable cubeValue = new FloatWritable();
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        	List<KeyValue> kvlist = value.getColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
        	for(KeyValue kv: kvlist){
        		String valueStr = Bytes.toString(kv.getValue());
        		String array[] = valueStr.split("\\|");
        		if(array.length == 7){
        			//name + "|" + mfgr + "|" + brand + "|" + type + "|" + size + "|" + container + "|" + retailprice;
        			String cubeKeyStr = array[1] + "|" + array[2] + "|" + array[3] + "|" + array[5];
        			float nvalue = Float.parseFloat(array[6]);
        			cubeKey.set(cubeKeyStr);
        			cubeValue.set(nvalue);
        			context.write(cubeKey, cubeValue);
        		}
        	}
	   	}
	}
	
	public static class CubeScanMapper extends Mapper<Object, Text, Text, FloatWritable> {
		private Text cubeKey = new Text("0");
		private FloatWritable cubeValue = new FloatWritable();
		public void map(Object key, Text value, Context context) throws IOException,   InterruptedException {
	        String s = value.toString();
	        String[] array = s.split("\\|");
	        if(array.length == 5){
    			String cubeKeyStr = array[0] + "|" + array[1] + "|" + array[2] + "|" + array[3];
    			float nvalue = Float.parseFloat(array[4]);
    			cubeKey.set(cubeKeyStr);
    			cubeValue.set(nvalue);
    			context.write(cubeKey, cubeValue);
	        }
	        
	    }
	}
	
	public static class CubeAggregationReducer extends Reducer  <Text, FloatWritable, Text, NullWritable> {
		NullWritable nullValue = NullWritable.get();
	    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
	            throws IOException, InterruptedException {
	        String ks = key.toString();
	        float sum = 0;
	        for (FloatWritable val : values){
	            sum += val.get();
	        }
	        context.write(new Text(key.toString() + "|" + sum), nullValue);

	    }
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
		long start = System.currentTimeMillis();
		/*
		DateFormat  formatter = new SimpleDateFormat("MM-dd-yyyy:HH-mm-ss");
		Date startDate;
		Date endDate;
		startDate = (Date) formatter.parse(args[0]);
		if(args[1].equals("now"))
			endDate = new Date();
		else
			endDate = (Date) formatter.parse(args[1]);
		*/
		
		
		String datacubeName = args[2];
		String tableName = args[3];
		Path inputPath1 = new Path(datacubeName);
		Path inputPath2 = new Path(tableName);
		Path outputPath = new Path(args[4]);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
		    fs.delete(outputPath);	
		}
		
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config,"Simulate Real-Time Scan");
		job.setJarByClass(IncreQuerying.class);     // class that contains mapper and reducer

		//Scan scan1 = new Scan();
		//Scan scan1 = new Scan(startDate.getTime(), endDate.getTime());
		Scan scan1 = new Scan(Long.parseLong(args[0]), Long.parseLong(args[1]));
		
		scan1.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan1.setCacheBlocks(false);  // don't set to true for MR jobs
			        
		TableMapReduceUtil.initTableMapperJob(
			tableName,        // input table
			scan1,
			RealTimeScanMapper.class,     // mapper class
			Text.class,         // mapper output key
			FloatWritable.class,  // mapper output value
			job);

		//TableMapReduceUtil.initTableReducerJob(datacubeName, null, job);
		//job.setNumReduceTasks(0);    // at least one, adjust as required
		
		job.setReducerClass(CubeAggregationReducer.class);    // reducer class
		job.setOutputFormatClass(TextOutputFormat.class);   
		
		MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, CubeScanMapper.class);
		MultipleInputs.addInputPath(job, inputPath2,  TableInputFormat.class, RealTimeScanMapper.class);

		FileOutputFormat.setOutputPath(job, outputPath); 
		job.waitForCompletion(true);

		/*
		TableMapReduceUtil.addDependencyJars(job);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		} */   
		
		long end=System.currentTimeMillis();
		long time = end - start;
		System.out.println("running time:  " + time);
		
		/*
		Date date = new Date();
		System.out.println("start: " + startDate.toLocaleString() + "  timestamp:  " + startDate.getTime());
		System.out.println("data cube refreshing time:" + endDate.toLocaleString() + "  timestamp:  " + endDate.getTime());
		System.out.println( "data cube refreshed time: " + date.toLocaleString() + "  timestamp:  " + date.getTime());
		*/
	}
}
