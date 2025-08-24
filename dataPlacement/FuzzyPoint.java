package dataPlacement;

public class FuzzyPoint {
	protected double dep;
	protected String name;
	protected double size;
	
	public FuzzyPoint(double dep, String name, double size){
		this.dep=dep;
		this.name=name;
		this.size=size;
	}
	
	public String toString3(){
		String result;
		result = "dataset: "+name+" dependency: "+dep+" with size: "+size;
		return result;
	}

}
