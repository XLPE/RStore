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

public class SimulateRealtimeQueryingLineitem {

	public static class QueryMapper extends
			TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		private LongWritable ts = new LongWritable(1);

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			List<KeyValue> kvlist = value.getColumn(Bytes.toBytes("f1"),
					Bytes.toBytes("c1"));
			int i = 0;
			String dimensionAttr = null;
			for (KeyValue kv : kvlist) {
				String valueStr = Bytes.toString(kv.getValue());
				String array[] = valueStr.split("\\|");

				String quantityStr = array[1]; // ****
				float quantity = Float.parseFloat(quantityStr);
				String extendepriceStr = array[2]; // **************
				float extendedprice = Float.parseFloat(extendepriceStr);
				String discountStr = array[3];
				float discount = Float.parseFloat(discountStr);
				String taxStr = array[4];
				float tax = Float.parseFloat(taxStr);
				float disc_price = extendedprice * (1 - discount);
				float charge = extendedprice * (1 - discount) * (1 + tax);
				String shipdate = array[7];
				if (shipdate.compareTo("1998-01-01") >= 0) {
					continue;
				}
				if (dimensionAttr == null) {
					dimensionAttr = array[5] + "|" + array[6];
					context.write(
							new ImmutableBytesWritable(Bytes
									.toBytes(dimensionAttr)),
							new ImmutableBytesWritable(Bytes
									.toBytes(quantityStr + "|" + extendedprice
											+ "|" + disc_price + "|" + charge)));
				} else {
					context.write(
							new ImmutableBytesWritable(Bytes
									.toBytes(dimensionAttr)),
							new ImmutableBytesWritable(Bytes
									.toBytes((0 - quantity) + "|"
											+ (0 - extendedprice) + "|"
											+ (0 - disc_price) + "|"
											+ (0 - charge))));
				}
			}
		}
	}

	public static class QueryReducer
			extends
			TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		public void reduce(ImmutableBytesWritable key,
				Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {
			float quantity = 0;
			float extendsdprice = 0;
			float sum_disc_price = 0;
			float sum_charge = 0;
			int count = 0;
			for (ImmutableBytesWritable val : values) {
				String valueStr = Bytes.toString(val.get());
				String array[] = valueStr.split("\\|");
				quantity += Float.parseFloat(array[0]);
				extendsdprice += Float.parseFloat(array[1]);
				sum_disc_price += Float.parseFloat(array[2]);
				sum_charge += Float.parseFloat(array[3]);
				count += 1;
			}
			Put put = new Put(key.copyBytes());
			put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"),
					Bytes.toBytes(quantity + "|" + extendsdprice + "|" + sum_disc_price + "|" + sum_charge + "|" + count));
			context.write(null, put);
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException, ParseException {
		long start = System.currentTimeMillis();

		DateFormat formatter = new SimpleDateFormat("MM-dd-yyyy:HH-mm-ss");
		Date startDate;
		Date endDate;
		startDate = (Date) formatter.parse(args[0]);
		if (args[1].equals("now"))
			endDate = new Date();
		else
			endDate = (Date) formatter.parse(args[1]);

		String datacubeName = args[2];
		int numOfReducers = Integer.parseInt(args[3]);
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "Simulate Real-Time querying");
		job.setJarByClass(ScanTwoTable.class); // class that contains mapper and
												// reducer

		Scan scan1 = new Scan(startDate.getTime(), endDate.getTime());

		scan1.setCaching(500); // 1 is the default in Scan, which will be bad
								// for MapReduce jobs
		scan1.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob("line", // input table
				scan1, QueryMapper.class, // mapper class
				ImmutableBytesWritable.class, // mapper output key
				ImmutableBytesWritable.class, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(datacubeName, // output table
				QueryReducer.class, // reducer class
				job);
		job.setNumReduceTasks(numOfReducers); // at least one, adjust as
												// required

		TableMapReduceUtil.addDependencyJars(job);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

		long end = System.currentTimeMillis();
		long time = end - start;
		System.out.println("running time:  " + time);

		Date date = new Date();
		System.out.println("start: " + startDate.toLocaleString()
				+ "  timestamp:  " + startDate.getTime());
		System.out.println("data cube refreshing time:"
				+ endDate.toLocaleString() + "  timestamp:  "
				+ endDate.getTime());
		System.out.println("data cube refreshed time: " + date.toLocaleString()
				+ "  timestamp:  " + date.getTime());
	}
}
