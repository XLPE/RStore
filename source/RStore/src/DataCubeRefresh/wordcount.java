package DataCubeRefresh;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HStreamingJobConf;
import org.apache.hadoop.mapreduce.HStreamingJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextHStreamingInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextHStreamingOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import DataCubeRefresh.PartStore.Part;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

/**
 * Counts tokenized words from input and counts them.
 */
public class wordcount extends Configured implements Tool {

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
		private List<List<Integer>> cuboids;

		/** {@inheritDoc} */

		@Override
		public void setup(Context context) {
			File dbHome = new File("/data0/lifeng/berkeleydb/"
					+ context.getTaskAttemptID());
			if (!dbHome.exists())
				dbHome.mkdirs();
			kvStore = new PartStore(dbHome);
			cuboids = iterateArray(1, 5);
		}

		private List<List<Integer>> iterateArray(int start, int end) {
			if (start == end) {
				List<List<Integer>> keys = new LinkedList<List<Integer>>();
				List<Integer> key = new LinkedList<Integer>();
				key.add(start);
				keys.add(key);
				return keys;
			} else {
				List<List<Integer>> tmpKeys = iterateArray(start + 1, end);
				List<List<Integer>> keys = new LinkedList<List<Integer>>();
				for (List<Integer> key : tmpKeys) {
					keys.add(key);
					List<Integer> newKey = new LinkedList<Integer>();
					newKey.add(start);
					for (Integer pos : key)
						newKey.add(pos);
					keys.add(newKey);
				}
				List<Integer> aloneKey = new LinkedList<Integer>();
				aloneKey.add(start);
				keys.add(aloneKey);
				return keys;
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String valueStr = value.toString();
			String array[] = valueStr.split("\\|");
			for (List<Integer> dimensionAttrPos : cuboids) {
				String dimensionAttr = "";
				for (int i = 0; i < dimensionAttrPos.size(); i++) {
					if (i == dimensionAttrPos.size() - 1)
						dimensionAttr += array[dimensionAttrPos.get(i)];
					else
						dimensionAttr += array[dimensionAttrPos.get(i)] + "|";
				}
				word.set(dimensionAttr);
				nvalue.set(Float.parseFloat(array[7]));
				context.write(word, nvalue);
			}
			int partKey = Integer.parseInt(array[0]);
			kvStore.pa.partByKey.put(new Part(partKey, value.toString(), 1001));
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

		float allSum = 0;

		/** {@inheritDoc} */
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float sum = 0;
			for (FloatWritable val : values) {
				sum += 1;
			}

			allSum += sum;
		}

		@Override
		public void cleanup(Context context) {

			try {
				context.write(new Text("count"), new FloatWritable(allSum));
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
		job.setJarByClass(wordcount.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setNumReduceTasks(Integer.parseInt(args[2]));
		HStreamingJobConf.setIsStreamingJob(job, true);
		job.setInputFormatClass(TextHStreamingInputFormat.class);
		job.setOutputFormatClass(TextHStreamingOutputFormat.class);

		TextHStreamingOutputFormat.setOutputPath(job, args[0]);

		for (int i = 3; i < args.length; i++) {
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
		System.exit(ToolRunner.run(new Configuration(), new wordcount(), args));
	}
}