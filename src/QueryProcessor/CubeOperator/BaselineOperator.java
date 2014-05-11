package QueryProcessor.CubeOperator;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
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

import QueryProcessor.DataCube;

public class BaselineOperator {
	public static class FullScanMapper extends
			TableMapper<Text, FloatWritable> {
		private LongWritable ts = new LongWritable(1);
		private Text groupKey = new Text("0");
		private FloatWritable groupValue = new FloatWritable();
		DataCube dc;

		@Override
		public void setup(Context context) {
			dc = new DataCube();
			Configuration conf = context.getConfiguration();
			dc = DataCube.deSerialize(conf.get("datacube"));

		}

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			List<KeyValue> kvlist = value.getColumn(Bytes.toBytes("f1"),
					Bytes.toBytes("c1"));
			for (KeyValue kv : kvlist) {
				String valueStr = Bytes.toString(kv.getValue());
				String array[] = valueStr.split("\\|");
				if (array.length == dc.table.numOfColumn) {
					// name + "|" + mfgr + "|" + brand + "|" + type + "|" + size
					// + "|" + container + "|" + retailprice;
					if (dc.cubeFilter.passFilter(array, dc.table)) {
						String cubeKeyStr = dc.groupBy.getGroupKey(array,
								dc.table);
						float nvalue = Float
								.parseFloat(array[dc.table.numOfColumn - 1]);
						groupKey.set(cubeKeyStr);
						groupValue.set(nvalue);
						context.write(groupKey, groupValue);
					}
				}
			}
		}
	}

	public static class BaselineReducer extends
			Reducer<Text, FloatWritable, Text, NullWritable> {
		DataCube dc;

		@Override
		public void setup(Context context) {
			dc = new DataCube();
			Configuration conf = context.getConfiguration();
			dc = DataCube.deSerialize(conf.get("datacube"));
		}

		NullWritable nullValue = NullWritable.get();

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			String ks = key.toString();
			float sum = 0;
			for (FloatWritable val : values) {
				sum += val.get();
			}
			context.write(new Text(key.toString() + "|" + sum), nullValue);
		}
	}

	public static void exe(DataCube dc) throws IOException,
			InterruptedException, ClassNotFoundException, ParseException {
		String datacubeName = dc.cuboid.cuboidName;
		String tableName = dc.tableName;
		//Path inputPath1 = new Path(datacubeName);
		
		Path inputPath1 = dc.cubePath;
		Path inputPath2 = new Path(tableName);
		Path outputPath = new Path(dc.outputPath);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath);
		}

		Configuration config = HBaseConfiguration.create();		
		config.set("datacube", dc.serialize());
		Job job = new Job(config, "Baseline Operator");
		job.setJarByClass(BaselineOperator.class); // class that contains
														// mapper and reducer         
		 Scan scan = new Scan();
		// Scan scan1 = new Scan(startDate.getTime(), endDate.getTime());
		//Scan scan = new Scan(dc.cubeRefreshTime, dc.queryingTime);

		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob(tableName, // input table
				scan, FullScanMapper.class, // mapper class
				Text.class, // mapper output key
				FloatWritable.class, // mapper output value
				job);

		job.setReducerClass(BaselineReducer.class); // reducer class
		job.setOutputFormatClass(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
	}
}
