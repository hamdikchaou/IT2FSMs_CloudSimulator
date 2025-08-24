package dataPlacement;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import fuzzy.Fuzzy_data;
import fuzzy_param.Fuzzy_function;
import fuzzy_param.Fuzzy_parametre;
import outil.Center;
import outil.Point;

public class FCM {
	
	private static int essaie_number=1;
	private static int num_cluster;
	private static double fuzziness=2;
	private static double epsilon=0.009;
	
	private ArrayList<DataCenter> fUsedDatacenters;
	private DataSet aNewDataset;
	//private int dependancy;
	
	private double[][] matrix;
	
	public FCM(DataSet aNewDataset,ArrayList<DataCenter> fUsedDatacenters) {
		this.fUsedDatacenters = fUsedDatacenters;
		this.aNewDataset = aNewDataset;
		this.num_cluster = fUsedDatacenters.size();
		matrix = new double[essaie_number][num_cluster];
	}
	
	public void init()
	{
		for(int i=0;i<essaie_number;i++){
			for(int j=0;j<num_cluster;j++){
				matrix[i][j]=calculateClustering(aNewDataset, fUsedDatacenters.get(j));
			}
		}
	}
	
	protected int calculateClustering(DataSet aDataset, DataCenter aDatacenter)
	{
		int dependancy = 0;
		
		//for each dataset in the center, sum it's clustering with the given set
		for(int i = 0; i < aDatacenter.getDatasets().size(); i++)
		{
			dependancy += Matrix.calculateDependancyFuzzy(aDataset, aDatacenter.getDatasets().get(i));
		}
		
		return dependancy;
	}
	
	public void membership_matrix()
	{
		for(int i=0;i<essaie_number;i++){
	    	double result=0.0;
			for(int j=0;j<num_cluster;j++){
				System.out.print(matrix[i][j]+"|");
				result=result+Math.pow(1/matrix[i][j],1/(fuzziness-1));
			}
			System.out.println();
			for(int j=0;j<num_cluster;j++){
				matrix[i][j]=Fuzzy_function.floorconvert(Math.pow(1/matrix[i][j],1/(fuzziness-1))/result);
				
				//affichage dependency fuzzy
				System.out.print(matrix[i][j]+"|");
			}
			System.out.println();
		}
	}
	
	public void cendroid_converge(){
		boolean cond=true;
		loops:
		while(cond){
			init();
			membership_matrix();
			List<Center> center=new_cendroid();
			
			for(int j=0;j<num_cluster;j++){
				Double x1=center.get(j).getX();
				//Double y1=center.get(j).getY();
				if(x1.isNaN()){
					break loops;
				}
				if(Math.abs(center.get(j).getX()-num_cluster)<epsilon ){
					cond=false;
				}
				else{
					cond=true;
				}
			}
			//fuzzy_data.setCluster_point(center);
		}
	}
	
	public List<Center> new_cendroid()
	{
		List<Center> center_point=new LinkedList<>();
		for(int j=0;j<num_cluster;j++){
			double result=0.0,x=0.0;
		for(int i=0;i<essaie_number;i++){
			result=result+Math.pow(matrix[i][j],fuzziness);
		}
		for(int i=0;i<essaie_number;i++){
			x=x+Math.pow(matrix[i][j],fuzziness)*matrix[i][j];
		}
		center_point.add(new Center(x/result));
		//System.out.println(j+"    "+" X="+x+"   "+result+"    "+ x/result);
	}
		return center_point;
	}
	
	public DataCenter cluster(DataCenter aDataCenterSource)
	{
		DataCenter bestcenter = null;
		int[] clust=new int[num_cluster];
		for(int i=0;i<num_cluster;i++){
			clust[i]=num_cluster;
		}
		
		double value=0;		
		for(int i=0;i<essaie_number;i++){
			int max=0;
			for(int j=0;j<num_cluster;j++){
				if(matrix[i][j]>value){
					max=j;
					value=matrix[i][j];
				}
			}
		}

		Double thisutilisation = aDataCenterSource.utilisation();
		Double bestutilisation = thisutilisation;
		for(int i=0;i<essaie_number;i++){
			for(int j=0;j<num_cluster;j++){
				if(matrix[i][j]==value){
					if(fUsedDatacenters.get(j).utilisation()< bestutilisation){
						bestutilisation= fUsedDatacenters.get(j).utilisation();
						bestcenter = fUsedDatacenters.get(j);
					}
				}
			}
		}
		if(bestutilisation==thisutilisation){
			bestcenter = aDataCenterSource;
		}
		
		return bestcenter;
	}	

}
