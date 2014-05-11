package DataCubeRefresh;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;


public class LoadPartTable {

	/**
	 * @param args
	 */

	Random rand = new Random();
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

	public LoadPartTable() {
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

	public String generatePartValue() {
		String name = getName(35);
		String mfgr = "Manufacturer#" + (1 + rand.nextInt(5));
		String brand = "Brand#" + (1 + rand.nextInt(55));
		String type = brandSize[rand.nextInt(brandSize.length)] + " "
				+ brandSkin[rand.nextInt(brandSkin.length)] + " "
				+ brandType[rand.nextInt(brandType.length)];   //120
		String size = "" + (1 + rand.nextInt(50));
		String container = containerSize[rand.nextInt(containerSize.length)]
				+ " " + containerType[rand.nextInt(containerType.length)];   //40
		double temp = (rand.nextFloat() * 1100.0) + 900;
		DecimalFormat df = new DecimalFormat("0.## ");
		String retailprice = "" + df.format(temp);
		return name + "|" + mfgr + "|" + brand + "|" + type + "|" + size + "|"
				+ container + "|" + retailprice;
	}

}
