package Tool;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
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

public class BuildFullCube {
	
	public static class BuildingCubeMapper extends TableMapper<ImmutableBytesWritable,FloatWritable> {
		private FloatWritable nvalue = new FloatWritable();
		private List<List<Integer> > cuboids;
		public void setup(Context context){
			cuboids = iterateArray(1,5);
		}
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {	        	
	        	List<KeyValue> kvlist = value.getColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
	        	for(KeyValue kv: kvlist){
	        		Map<String, Object> smap = kv.toStringMap();
					String valueStr = Bytes.toString(kv.getValue());
					String array[] = valueStr.split("\\|");
					for (List<Integer> dimensionAttrPos : cuboids) {
						String dimensionAttr = "";
						for(int i = 0; i < dimensionAttrPos.size(); i ++){
							if(i == dimensionAttrPos.size() - 1)
								dimensionAttr += array[dimensionAttrPos.get(i)];
							else
								dimensionAttr += array[dimensionAttrPos.get(i)] + "|";
						}
						row.set(Bytes.toBytes(dimensionAttr));
						nvalue.set(Float.parseFloat(array[6]));
						context.write(row, nvalue);
					}	        		
	        	}
	   	}
	   	
	   	private List<List<Integer> > iterateArray(int start, int end) {
			if (start == end) {
				List<List<Integer> > keys = new LinkedList<List<Integer> >();
				List<Integer> key = new LinkedList<Integer>();
				key.add(start);
				keys.add(key);
				return keys;
			} else {
				List<List<Integer> > tmpKeys = iterateArray(start + 1, end);
				List<List<Integer> > keys = new LinkedList<List<Integer>>();
				for (List<Integer> key : tmpKeys) {
					keys.add(key);
					List<Integer> newKey = new LinkedList<Integer>();
					newKey.add(start);
					for(Integer pos: key)
						newKey.add(pos);
					keys.add(newKey);
				}
				List<Integer> aloneKey = new LinkedList<Integer>();
				aloneKey.add(start);
				keys.add(aloneKey);
				return keys;
			}
		}
	}
	
	public static class BuildingCubeCombiner extends TableReducer<ImmutableBytesWritable, FloatWritable, ImmutableBytesWritable> {
		FloatWritable output = new FloatWritable();
		public void reduce(ImmutableBytesWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			float i = 0;
    		for (FloatWritable val : values) {
    			i += val.get();
    		}
    		output.set(i);
    		context.write(key, output);
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
		Job job = new Job(config,"Build full Data Cube");
		job.setJarByClass(BuildFullCube.class);     // class that contains mapper and reducer
			        
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
		job.setCombinerClass(BuildingCubeCombiner.class);
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
