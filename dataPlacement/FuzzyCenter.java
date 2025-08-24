package dataPlacement;

import java.util.LinkedList;
import java.util.List;

public class FuzzyCenter extends FuzzyPoint{
	
	private List<FuzzyPoint> data_point;

	public FuzzyCenter(double dep, String name, double size) {
		super(dep, name, size);
	}
	
	public void setSize(double size){
		this.size = size;
	}
	
/*	public void setDataPoint(LinkedList<FuzzyPoint> dp){
		data_point = dp ;
	}*/
	
/*	public void setfreeSize(double size){
		this.size = 0.0;
	}*/
	
	public String toString3(){
		String result;
		result = "DataCenter: "+name+" dependency_center: "+dep+" cumulative_size: "+size;
		return result;
	}

}
