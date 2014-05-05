package QueryProcessor.CubeOperator;

import QueryProcessor.DataCube;


public class SimpleCostModel extends CostModel{

	DataCube dc;
	@Override
	public boolean isIncreQueryingBetter() {
		if(dc.updateRatio < 0.25)
		    return true;
		return false;
	}
	public SimpleCostModel(DataCube dc){
		this.dc = dc;					
	}

}
