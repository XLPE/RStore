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
import DataCubeRefresh.KeyValueStore.KeyValue;
import QueryProcessor.DataCube;
import QueryProcessor.ROLAPUtil;

import com.sleepycat.persist.EntityCursor;

/**
 * Counts tokenized words from input and counts them.
 */
public class CubeRefresher extends Configured implements Tool {

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
		KeyValueStore kvStore;
		private FloatWritable nvalue = new FloatWritable();
		private FloatWritable nvalue2 = new FloatWritable();
		DataCube dc;
		String dbHomeStr;

		/** {@inheritDoc} */

		@Override
		public void setup(Context context) {
			dbHomeStr = context.getConfiguration().get("dbHome");
			File dbHome = new File(dbHomeStr + "/part"
					+ context.getTaskAttemptID());
			if (!dbHome.exists())
				dbHome.mkdirs();
			kvStore = new KeyValueStore(dbHome);

			dc = new DataCube();
			Configuration conf = context.getConfiguration();
			dc = DataCube.deSerialize(conf.get("datacube"));
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String valueStr = value.toString();
			String array[] = valueStr.split("@");
			if (array.length != 2)
				return;
			int tuplekey = Integer.parseInt(array[0]);
			String tuplevalue = array[1];
			array = tuplevalue.split("\\|");
			if(array.length != dc.table.numOfColumn)
				return;
			String cubeKey = dc.cuboid.getCubeKey(array, dc.table);
			float cubeValue = dc.cuboid.getCubeValue(array, dc.table);
			KeyValue oldTuple = kvStore.pa.partByKey.get(tuplekey);
			// key does not exists, so it is an insertion
			if (oldTuple == null) {
				kvStore.pa.partByKey.put(new KeyValue(tuplekey, cubeKey,
						cubeValue));
				word.set(cubeKey);
				nvalue.set(cubeValue);
				context.write(word, nvalue);
			}
			// key exists, so it is an update
			else {
				if (oldTuple.dKey.equals(cubeKey)) {
					// only numerical value is changed
					if (oldTuple.nValue != cubeValue) {
						word.set(cubeKey);
						nvalue.set(cubeValue);
						context.write(word, nvalue);
						kvStore.pa.partByKey.put(new KeyValue(tuplekey, cubeKey,
								cubeValue));
					}
				} else {
					// both dimension and numerical value is changed
					word.set(cubeKey);
					nvalue.set(cubeValue);
					context.write(word, nvalue);
					nvalue2.set(0 - oldTuple.nValue);
					context.write(word, nvalue2);
					kvStore.pa.partByKey.put(new KeyValue(tuplekey, cubeKey,
							cubeValue));
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

		// Configuration config;
		// HTable table;

		FileSystem fs = null;

		@Override
		public void setup(Context context) throws IOException {
			String dbHomeStr = context.getConfiguration().get("dbHome");
			File dbHome = new File(dbHomeStr + "/cube"
					+ context.getTaskAttemptID());
			if (!dbHome.exists())
				dbHome.mkdirs();
			cubeStore = new CubeStore(dbHome);

			dbHome = new File(dbHomeStr + "/deltacube"
					+ context.getTaskAttemptID());
			if (!dbHome.exists())
				dbHome.mkdirs();
			deltaCubeStore = new CubeStore(dbHome);
			// refreshTime = context.getConfiguration().getLong("initialTime",
			// 0);
			refreshInterval = context.getConfiguration().getLong(
					"refreshInterval", 0);

			String hdfsAddress = context.getConfiguration().get("hdfsaddress");
			// config = HBaseConfiguration.create();
			// config.addResource(new
			// Path("/home/lifeng/ROLAP/hbase-stable/target/hbase-0.92.1/hbase-0.92.1/conf/hbase-site.xml"));
			Configuration conf = new Configuration();
			// fs = FileSystem.get(conf);
			try {
				fs = FileSystem.get(new URI(hdfsAddress), conf);
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			allSum++;
			// table = new HTable(config, "cube");

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
			if (c == null) {
				deltaCubeStore.ca.cubeByKey.put(new Cube(cubeKey, sum));
			} else {
				c.nValue = c.nValue + sum;
				deltaCubeStore.ca.cubeByKey.put(c);
			}
			Date curDate = new Date();
			// curDate.
			// check whether need to refresh data cube
			Long curTime = System.currentTimeMillis();
			if ((curTime - refreshTime) >= refreshInterval * 1000) {
				// System.out.println("old refresh time: " + refreshTime);
				refreshTime = (curTime / (refreshInterval * 1000))
						* (refreshInterval * 1000);
				// System.out.println("curTime: " + curTime);
				// System.out.println(refreshTime + ":  " + (refreshTime +
				// refreshInterval*1000));
				// System.out.println("cur time: " + (new
				// Date()).toGMTString());
				// update data cube using cuboid
				EntityCursor<Cube> cursor = deltaCubeStore.ca.cubeByKey
						.entities();
				for (Cube deltaCell : cursor) {
					Cube cell = cubeStore.ca.cubeByKey.get(deltaCell.cubeKey);
					if (cell != null) {
						cell.nValue = cell.nValue + deltaCell.nValue;
						cubeStore.ca.cubeByKey.put(cell);
					} else {
						cubeStore.ca.cubeByKey.put(deltaCell);
					}
				}
				cursor.close();
				// write the datacube into hdfs
				cursor = cubeStore.ca.cubeByKey.entities();

				SimpleDateFormat dt = new SimpleDateFormat(
						"z_yyyy.MM.dd_hh.mm.ss");
				dt.setTimeZone(TimeZone.getTimeZone("PST"));
				String dateStr = ROLAPUtil.getTimeStr(refreshTime);

				String cubeDirectory = "/cube" + "/" + dateStr;
				Path dir = new Path(cubeDirectory);
				if (!fs.exists(dir)) {
					fs.mkdirs(dir);
				}
				Path file = new Path(cubeDirectory + "/"
						+ context.getTaskAttemptID());
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
						fs.create(file, true)));
				for (Cube cell : cursor) {
					String line = cell.cubeKey + "|" + cell.nValue + "\n";
					bw.write(line);
				}
				bw.close();
				cursor.close();
				// System.out.println("time after refresh: " + (new
				// Date()).toGMTString());
				deltaCubeStore.restart();

				// write the data cube into HBase
				/*
				 * cursor = cubeStore.ca.cubeByKey.entities(); int batchFactor =
				 * 200; int k = 0; List<Put> batch = new ArrayList<Put>();
				 * for(Cube cell: cursor){ k ++; Put p = new
				 * Put(Bytes.toBytes(cell.cubeKey)); p.add(Bytes.toBytes("f1"),
				 * Bytes.toBytes("c1"), Bytes.toBytes(cell.nValue + ""));
				 * batch.add(p); if(k%batchFactor == 0){ table.put(batch);
				 * batch.clear(); } } if(batch.size() > 0) table.put(batch);
				 */
			}
		}

		@Override
		public void cleanup(Context context) {
			deltaCubeStore.close();
			cubeStore.close();
		}
	}

	/** {@inheritDoc} 
	 * 0: dir for berkeylydb
	 * 1: reduce interval (ms)
	 * 2: num of reducer
	 * 3: cube refresh interval (s) 
	 * 4: table name
	 * 5: schema path
	 * 6: data cube output hdfs address
	 * 7: hstreaming output file address
	 * */
	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = args;

		
	
		// fixed time interval of 1s
		int interval = Integer.parseInt(args[1]);

		Job job = new Job(getConf(), "Streaming Word Count");
		job.setJarByClass(CubeRefresher.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setNumReduceTasks(Integer.parseInt(args[2]));
		long refreshInterval = Long.parseLong(args[3]);
		
		

		Long curTime = System.currentTimeMillis();

		//"part", "schemaPath"
		DataCube dc = new DataCube(args[4], args[5]);
		//"/data1/lifeng/berkeleydb"
		job.getConfiguration().set("dbHome", args[0]);
		job.getConfiguration().setLong("initialTime", curTime);
		job.getConfiguration().setLong("refreshInterval", refreshInterval);
		//hdfs://awan-0-00-0:44111
		job.getConfiguration().set("hdfsaddress", args[6]);
		job.getConfiguration().set("datacube", dc.serialize());

		HStreamingJobConf.setIsStreamingJob(job, true);
		job.setInputFormatClass(TextHStreamingInputFormat.class);
		job.setOutputFormatClass(TextHStreamingOutputFormat.class);
		//TextHStreamingOutputFormat.set
		//TextHStreamingOutputFormat.setOutputPath(job, "hdfs://awan-0-02-0:34111/stream");
		TextHStreamingOutputFormat.setOutputPath(job, args[7]);

		for (int i = 8; i < args.length; i++) {
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
		System.exit(ToolRunner.run(new Configuration(), new CubeRefresher(),
				args));
	}
}