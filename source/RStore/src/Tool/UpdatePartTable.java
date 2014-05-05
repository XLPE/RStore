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

public class UpdatePartTable {

	/**
	 * @param args
	 */

	long maxKey;
	long numOfKeys;
	Random keyRand = new Random();
	Random rand = new Random();
	List<Long> keyList;
	private final char[] alphanumeric = alphanumeric();

	private final String[] brandSize = { "LARGE", "STANDARD", "SMALL", "PROMO",
			"MEDIUM", "ECONOMY" };
	private final String[] brandSkin = { "PLATED", "ANODIZED", "POLISHED",
			"BRUSHED" };
	private final String[] brandType = { "COPPER", "NICKEL", "TIN", "STEEL",
			"BRASS" };

	private final String[] containerSize = { "SM", "LG", "JUMBO", "WRAP", "MED" };
	private final String[] containerType = { "CASE", "PACK", "DRUM", "BAG",
			"JAR", "BOX", "PKG", "CAN" };

	public UpdatePartTable(long maxKey, long numOfKeys) {
		this.maxKey = maxKey;
		this.numOfKeys = numOfKeys;
		keyList = new LinkedList();
		for (long i = 0; i < numOfKeys; i++) {
			long curKey = 1 + keyRand.nextLong()%maxKey;
			keyList.add(curKey);
		}
	}

	private char[] alphanumeric() {
		StringBuffer buf = new StringBuffer(128);
		for (int i = 48; i <= 57; i++)
			buf.append((char) i); // 0-9
		for (int i = 65; i <= 90; i++)
			buf.append((char) i); // A-Z
		for (int i = 97; i <= 122; i++)
			buf.append((char) i); // a-z
		return buf.toString().toCharArray();
	}

	public String getName(int len) {
		StringBuffer out = new StringBuffer();
		while (out.length() < len) {
			int idx = Math.abs((rand.nextInt() % alphanumeric.length));
			out.append(alphanumeric[idx]);
		}
		return out.toString();

	}

	public String generatePartKey() {
		return (keyList.get(keyRand.nextInt((int) numOfKeys))).toString();
	}

	public String generatePartValue() {
		String name = getName(35);
		String mfgr = "Manufacturer#" + (1 + rand.nextInt(5));
		String brand = "Brand#" + (1 + rand.nextInt(55));
		String type = brandSize[rand.nextInt(brandSize.length)] + " "
				+ brandSkin[rand.nextInt(brandSkin.length)] + " "
				+ brandType[rand.nextInt(brandType.length)];
		String size = "" + (1 + rand.nextInt(50));
		String container = containerSize[rand.nextInt(containerSize.length)]
				+ " " + containerType[rand.nextInt(containerType.length)];
		double temp = (rand.nextFloat() * 1100.0) + 900;
		DecimalFormat df = new DecimalFormat("0.## ");
		String retailprice = "" + df.format(temp);
		return name + "|" + mfgr + "|" + brand + "|" + type + "|" + size + "|"
				+ container + "|" + retailprice;
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		long start = System.currentTimeMillis();
		String rowkey;
		String columnFamily;
		Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(config, "part");
		//table.setAutoFlush(false);
		//table.setWriteBufferSize(25165824);
		if (args.length < 3) {
			System.out.println("not enough parameters");
			System.exit(0);
		}
		long maxKey = Long.parseLong(args[0]);
		long numOfKey = Long.parseLong(args[1]);
		long numOfUpdate = Long.parseLong(args[2]);

		UpdatePartTable update = new UpdatePartTable(maxKey, numOfKey);
		long printout = numOfUpdate / 100;
		long batchFactor = 200;
		long t = 0;
		for (long i = 0; i < numOfUpdate / batchFactor; i++) {
			// System.out.println(update.generatePartKey() + "   " +
			// update.generatePartValue() );
			List<Put> batch = new ArrayList<Put>();
			if ((t) % printout == 0)
				System.out
						.println("percentage: " + ((t) / printout));
			for (int k = 0; k < batchFactor; k++) {
				Put p = new Put(Bytes.toBytes(update.generatePartKey()));
				p.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"),
						Bytes.toBytes(update.generatePartValue()));
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
