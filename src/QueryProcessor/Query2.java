package QueryProcessor;

public class Query2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//select * from part where p_type = "ECONOMY ANODIZED BRASS" 
		//and p_container = "WRAP PKG:
		//groupby p_brand
		DataCube dc = new DataCube("part", args[0]);		
		dc.addFilter("p_type", "=", "ECONOMY ANODIZED BRASS");
		dc.addFilter("p_container", "=", "WRAP PKG");
		//dc.addGroupBy("p_brand");
		dc.addGroupBy("p_type");
		dc.addGroupBy("p_container");
		dc.setOutputPath(args[1] + "/Query2");
		dc.execute();
	}

}
