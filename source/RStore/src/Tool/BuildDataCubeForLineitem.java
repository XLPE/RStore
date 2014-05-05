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

public class BuildDataCubeForLineitem {

	public static class BuildingCubeLineitemMapper extends
			TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			List<KeyValue> kvlist = value.getColumn(Bytes.toBytes("f1"),
					Bytes.toBytes("c1"));
			for (KeyValue kv : kvlist) {
				String valueStr = Bytes.toString(kv.getValue());
				String array[] = valueStr.split("\\|");
				String dimensionAttr = array[5] + "|" + array[6] + "|"
						+ array[7];

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
				context.write(
						new ImmutableBytesWritable(Bytes.toBytes(dimensionAttr)),
						new ImmutableBytesWritable(Bytes.toBytes(quantityStr
								+ "|" + extendedprice + "|" + disc_price
								+ "|" + charge)));
			}
		}
	}

	public static class BuildingCubeLineitemReducer
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

	public static void main(String args[]) throws ParseException, IOException,
			InterruptedException, ClassNotFoundException {
		long start = System.currentTimeMillis();
		String datacubeName = args[1];
		DateFormat formatter = new SimpleDateFormat("MM-dd-yyyy:HH-mm-ss");
		Date date;
		if (args[0].equals("now"))
			date = new Date();
		else
			date = (Date) formatter.parse(args[0]);

		int numOfReducers = Integer.parseInt(args[2]);
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "Build Data Cube for Lineitem");
		job.setJarByClass(BuildDataCubeForLineitem.class); // class that
															// contains mapper
															// and reducer

		Scan scan = new Scan();
		scan.setTimeRange(0, date.getTime());
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob("line", // input table
				scan, // Scan instance to control CF and attribute selection
				BuildingCubeLineitemMapper.class, // mapper class
				ImmutableBytesWritable.class, // mapper output key
				ImmutableBytesWritable.class, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(datacubeName, // output table
				BuildingCubeLineitemReducer.class, // reducer class
				job);
		job.setNumReduceTasks(numOfReducers); // at least one, adjust as
												// required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

		long end = System.currentTimeMillis();
		long time = end - start;

		System.out.println("running time:  " + time);

		System.out.println("datacube building time: " + date.toLocaleString()
				+ "  timestamp:  " + date.getTime());
		date = new Date();
		System.out.println("datacube built time: " + date.toLocaleString()
				+ "  timestamp:  " + date.getTime());

	}
}
