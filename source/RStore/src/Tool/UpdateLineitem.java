package Tool;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class UpdateLineitem {

	/**
	 * @param args
	 */

	int maxKey;
	int numOfKeys;
	Random keyRand = new Random();
	Random rand = new Random();
	List<Integer> keyList;

	private final String[] reutrnFlagArray = { "A", "R", "N" };
	private final String[] lineStatusArray = { "F", "O" };
	private final String[] yearArray = { "1992", "1993", "1994", "1995",
			"1996", "1997", "1998" };
	private final String[] shipInstructArray = { "DELIVER IN PERSON",
			"TAKE BACK RETURN", "NONE", "COLLECT COD" };
	private final String[] shiomodeArray = { "SHIP", "TRUCK", "MAIL", "FOB",
			"REG AIR", "RAIL" };

	public UpdateLineitem(int maxKey, int numOfKeys) {
		this.maxKey = maxKey;
		this.numOfKeys = numOfKeys;
		keyList = new LinkedList();
		for (int i = 0; i < numOfKeys; i++) {
			int curKey = 1 + keyRand.nextInt(maxKey);
			keyList.add(curKey);
		}
	}

	public String getDate() {
		String year = yearArray[rand.nextInt(7)];
		int monthInt = rand.nextInt(12) + 1;
		String month = "";
		if (monthInt < 10)
			month = "0" + monthInt;
		else
			month = "" + monthInt;
		int dayInt = rand.nextInt(30) + 1;
		String day = "";
		if (dayInt < 10)
			day = "0" + dayInt;
		else
			day = "" + dayInt;
		String dateStr = year + "-" + month + "-" + day;
		return dateStr;
	}

	public String generatePartKey() {
		return (keyList.get(keyRand.nextInt(numOfKeys))).toString();
	}

	public String generateLineitemValue() {
		String linenumber = "" + (1 + rand.nextInt(7)); // 0
		String quantity = "" + (1 + rand.nextInt(50)); // 1
		double temp = (rand.nextFloat() * 80000.0) + 1;
		DecimalFormat df = new DecimalFormat("0.##");
		String extendedprice = "" + df.format(temp); // 2
		String discount = 0.01 * (rand.nextInt(11)) + ""; // 3
		String tax = 0.01 * (rand.nextInt(9)) + ""; // 4

		String returnflag = reutrnFlagArray[rand.nextInt(3)]; // 5
		String linestatus = lineStatusArray[rand.nextInt(2)]; // 6
		String shipdate = getDate(); // 7

		String commitdate = getDate(); // 8
		String receiptdate = getDate(); // 9
		String shipinstruct = shipInstructArray[rand.nextInt(4)]; // 10
		String shipmode = shiomodeArray[rand.nextInt(6)]; // 11

		return linenumber + "|" + quantity + "|" + extendedprice + "|"
				+ discount + "|" + tax + "|" + returnflag + "|" + linestatus
				+ "|" + shipdate + "|" + commitdate + "|" + receiptdate + "|" + shipinstruct + "|" + shipmode;
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		long start = System.currentTimeMillis();
		String rowkey;
		String columnFamily;
		Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(config, "line");
		//table.setAutoFlush(false);
		//table.setWriteBufferSize(25165824);
		if (args.length < 3) {
			System.out.println("not enough parameters");
			System.exit(0);
		}
		int maxKey = Integer.parseInt(args[0]);
		int numOfKey = Integer.parseInt(args[1]);
		int numOfUpdate = Integer.parseInt(args[2]);

		UpdateLineitem update = new UpdateLineitem(maxKey, numOfKey);
		int printout = numOfUpdate / 100;
		int batchFactor = 200;
		int t = 0;
		for (int i = 0; i < numOfUpdate / batchFactor; i++) {
			// System.out.println(update.generatePartKey() + "   " +
			// update.generatePartValue() );
			List<Put> batch = new ArrayList<Put>();
			if ((t) % printout == 0)
				System.out
						.println("percentage: " + ((t) / printout));
			for (int k = 0; k < batchFactor; k++) {
				Put p = new Put(Bytes.toBytes(update.generatePartKey()));
				p.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"),
						Bytes.toBytes(update.generateLineitemValue()));
				//System.out.println(update.generatePartKey() + "  " + update.generateLineitemValue());
				batch.add(p);
				t ++;
			}
			table.put(batch);
		}

		long end = System.currentTimeMillis();
		long time = end - start;

		System.out.println("running time:  " + time);
		Date date = new Date();
		System.out.println("update finished time: " + date.toLocaleString()
				+ "  timestamp:  " + date.getTime());
	}
}
