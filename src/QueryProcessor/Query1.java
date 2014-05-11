package QueryProcessor;

public class Query1 {

	public static void main(String[] args) {
		DataCube dc = new DataCube("part", args[0]);		
		dc.addFilter("p_type", "=", "ECONOMY ANODIZED BRASS");
		dc.addFilter("p_container", "=", "WRAP PKG");
		dc.addGroupBy("p_brand");
		dc.updateRatio = 0.2;
		dc.setOutputPath(args[1] + "/Query1");
		dc.execute();
	}

}
