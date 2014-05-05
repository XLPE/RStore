package DataCubeRefresh;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HStreamingJobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextHStreamingInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextHStreamingOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import DataCubeRefresh.CubeStore.Cube;
import DataCubeRefresh.PartStore.Part;
import QueryProcessor.ROLAPUtil;

import com.sleepycat.persist.EntityCursor;

/**
 * Counts tokenized words from input and counts them.
 */
public class CubeUpdater extends Configured implements Tool {

	static final String validProtocols[] = { "http", "https", "tcp", "udp" };

	/**
	 * Inner mapper class.
	 */
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, FloatWritable> {
		private final static FloatWritable one = new FloatWritable(1);
		private Text word = new Text("0");
		Random rand = new Random();
		int i = 0;
		PartStore kvStore;
		private FloatWritable nvalue = new FloatWritable();
		private FloatWritable nvalue2 = new FloatWritable();

		/** {@inheritDoc} */

		@Override
		public void setup(Context context) {
			File dbHome = new File("/data1/lifeng/berkeleydb/part"
					+ context.getTaskAttemptID());
			if (!dbHome.exists())
				dbHome.mkdirs();
			kvStore = new PartStore(dbHome);
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String valueStr = value.toString();
			String array[] = valueStr.split("\\|");
			
			if(array.length != 8)
				return;
			String mfgr = array[2];
			String brand = array[3];
			String type = array[4];
			String size = array[5];
			String container = array[6];		
			int partKey = Integer.parseInt(array[0]);
			String cubeKey = mfgr+ "|" + brand + "|" + type + "|" + container;
			float cubeValue = Float.parseFloat(array[7]);			
			Part oldPart = kvStore.pa.partByKey.get(partKey);
			//key does not exists, so it is an insertion
			if(oldPart == null){
				
				kvStore.pa.partByKey.put(new Part(partKey, cubeKey, cubeValue));				
				word.set(cubeKey);
				nvalue.set(cubeValue);
				context.write(word, nvalue);
			}
			//key exists, so it is an update
			else{								
				if(oldPart.dKey.equals(cubeKey)){
					//only numerical value is changed
					if(oldPart.nValue != cubeValue){
						word.set(cubeKey);
						nvalue.set(cubeValue);
						context.write(word, nvalue);
						kvStore.pa.partByKey.put(new Part(partKey, cubeKey, cubeValue));
					}
				}
				else{
					//both dimension and numerical value is changed
					word.set(cubeKey);
					nvalue.set(cubeValue);
					context.write(word, nvalue);
					nvalue2.set(0 - oldPart.nValue);
					context.write(word, nvalue2);
					kvStore.pa.partByKey.put(new Part(partKey, cubeKey, cubeValue));
				}
			}			
		}

		@Override
		public void cleanup(Context context) {
			System.out.println("" + i);
			kvStore.close();
		}
	}

	/**
	 * Inner reducer class.
	 */
	public static class IntSumReducer extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {		

		static float allSum = 0;
		CubeStore cubeStore;
		CubeStore deltaCubeStore;
		static long refreshTime = 0;
		long refreshInterval;
		
		//Configuration config;
		//HTable table;
		
		FileSystem fs = null;

		@Override
		public void setup(Context context) throws IOException{
			File dbHome = new File("/data1/lifeng/berkeleydb/cube"
					+ context.getTaskAttemptID());
			if (!dbHome.exists())
				dbHome.mkdirs();
			cubeStore = new CubeStore(dbHome);
			
			dbHome = new File("/data1/lifeng/berkeleydb/deltacube"
					+ context.getTaskAttemptID());
			if (!dbHome.exists())
				dbHome.mkdirs();
			deltaCubeStore = new CubeStore(dbHome);
			//refreshTime = context.getConfiguration().getLong("initialTime", 0);
			refreshInterval = context.getConfiguration().getLong("refreshInterval", 0);
			
			//config = HBaseConfiguration.create();
			//config.addResource(new Path("/home/lifeng/ROLAP/hbase-stable/target/hbase-0.92.1/hbase-0.92.1/conf/hbase-site.xml"));
			Configuration conf = new Configuration();
			//fs = FileSystem.get(conf);		
			try {
				fs = FileSystem.get(new URI("hdfs://awan-0-00-0:44111"), conf);
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			allSum ++;
			//table = new HTable(config, "cube");
			
		}
		/** {@inheritDoc} */
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float sum = 0;
			String cubeKey = key.toString();
			for (FloatWritable val : values) {
				sum += val.get();				
			}
			Cube c = deltaCubeStore.ca.cubeByKey.get(cubeKey);
			if(c == null){
				deltaCubeStore.ca.cubeByKey.put(new Cube(cubeKey, sum));
			}
			else{				
				c.nValue = c.nValue + sum;
				deltaCubeStore.ca.cubeByKey.put(c);
			}
			Date curDate = new Date();
			//curDate.
			//check whether need to refresh data cube
			Long curTime = System.currentTimeMillis();
			if( (curTime - refreshTime) >= refreshInterval*1000){
				//System.out.println("old refresh time: " + refreshTime);				
				refreshTime = (curTime/(refreshInterval*1000))*(refreshInterval*1000);
				//System.out.println("curTime: " + curTime);				
				//System.out.println(refreshTime + ":  " + (refreshTime + refreshInterval*1000));
				//System.out.println("cur time: " + (new Date()).toGMTString());				
				//update data cube using cuboid
				EntityCursor<Cube> cursor =  deltaCubeStore.ca.cubeByKey.entities();
				for(Cube deltaCell: cursor){
					Cube cell = cubeStore.ca.cubeByKey.get(deltaCell.cubeKey);
					if(cell != null){
						cell.nValue = cell.nValue + deltaCell.nValue;
						cubeStore.ca.cubeByKey.put(cell);
					}
					else{
						cubeStore.ca.cubeByKey.put(deltaCell);
					}
				}
				cursor.close();
				//write the datacube into hdfs
				cursor = cubeStore.ca.cubeByKey.entities();
				
				SimpleDateFormat dt = new SimpleDateFormat("z_yyyy.MM.dd_hh.mm.ss");
				dt.setTimeZone(TimeZone.getTimeZone("PST"));
				String dateStr = ROLAPUtil.getTimeStr(refreshTime);
				
				String cubeDirectory = "/cube" + "/" + dateStr;
				Path dir= new Path(cubeDirectory);
				if(!fs.exists(dir)){
					fs.mkdirs(dir);
				}
				Path file = new Path(cubeDirectory + "/" + context.getTaskAttemptID());
				BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(file,true)));
				for(Cube cell: cursor){
					String line = cell.cubeKey + "|" + cell.nValue + "\n"; 
					bw.write(line);
				}
                bw.close();
                cursor.close();
                //System.out.println("time after refresh: " + (new Date()).toGMTString());
                deltaCubeStore.restart();
                
                

				
				//write the data cube into HBase
				/*
				cursor = cubeStore.ca.cubeByKey.entities();
				int batchFactor = 200;
				int k = 0;
				List<Put> batch = new ArrayList<Put>();
				for(Cube cell: cursor){
					k ++;
					Put p = new Put(Bytes.toBytes(cell.cubeKey));
					p.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(cell.nValue + ""));
					batch.add(p);
					if(k%batchFactor == 0){
						table.put(batch);
						batch.clear();
					}
				}
				if(batch.size() > 0)
					table.put(batch);
				*/
			}
		}

		@Override
		public void cleanup(Context context) {

			try {
				context.write(new Text("count"), new FloatWritable(allSum));
				context.write(new Text(new Date().toGMTString()), new FloatWritable(allSum));
				deltaCubeStore.close();
				cubeStore.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = args;

		// fixed time interval of 1s
		int interval = Integer.parseInt(args[1]);

		Job job = new Job(getConf(), "Streaming Word Count");		
		job.setJarByClass(CubeUpdater.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setNumReduceTasks(Integer.parseInt(args[2]));
		long refreshInterval = Long.parseLong(args[3]);
		
		Long curTime =  System.currentTimeMillis();;		
		
		job.getConfiguration().setLong("initialTime", curTime);
		job.getConfiguration().setLong("refreshInterval", refreshInterval);
		
		HStreamingJobConf.setIsStreamingJob(job, true);
		job.setInputFormatClass(TextHStreamingInputFormat.class);
		job.setOutputFormatClass(TextHStreamingOutputFormat.class);

		TextHStreamingOutputFormat.setOutputPath(job, args[0]);

		for (int i = 4; i < args.length; i++) {
			TextHStreamingInputFormat.addInputStream(job, interval, 600, -1,
					"", false, args[i]);
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Entry point.
	 * 
	 * @param args
	 *            command line arguments
	 * @throws Exception
	 *             if an exception occurs
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new CubeUpdater(), args));
	}
}