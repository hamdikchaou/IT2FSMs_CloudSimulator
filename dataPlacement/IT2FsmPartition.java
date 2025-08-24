package dataPlacement;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import fuzzy_param.Fuzzy_function;
import dataPlacement.FuzzyCenter;
import dataPlacement.FuzzyPoint;

public class IT2FsmPartition {
	private static int essaie_number;//number of datasets
	private static int num_cluster;//number of datacenters
	private static double fuzziness=2.4;
	private static double m1=1.4, m2=2.4;
	private static double after_dot=3;
	private static double epsilon=0.001;

	private double[][] matrix;
	private double[][] matrix2;
	private double[][] lower;
	private double[][] upper;
	private double[][] lu;
	private Matrix mat;
	private double dc_size;
	int compt=0;
	private ArrayList<DataCenter> fUsedDatacenters;
	private ArrayList<DataCenter> fuzzyUsedDatacenters;
	private ArrayList<DataSet> alldataset;
	private List<FuzzyPoint> data_point;
	public List<FuzzyCenter> cluster_point;
	public List<FuzzyCenter> cluster_point2;
	
	private List<FuzzyPoint>[] cluster;
	
	public IT2FsmPartition(ArrayList<DataCenter> fDatacenters,Matrix mat) throws Exception
	{
		
		
		this.fUsedDatacenters = fDatacenters;
		this.num_cluster = fUsedDatacenters.size();
		this.mat=mat;
		//this.matrix = new double[essaie_number][num_cluster];
		essaie_number=mat.getData().length;//nbre of datasets
		this.dc_size=fDatacenters.get(0).getSize()*fDatacenters.get(0).getP_ini();
		
		this.Fuzzy();//phase d'initialisation(avant la premiere etape)
		System.out.println("---------------Init_fuzzy_before_membership-------------");
		for(int j=0;j<num_cluster;j++){
			System.out.println(cluster_point.get(j).toString3());
		}
		System.out.println("");
		
		cluster=new List[num_cluster];
		for(int i=0;i<num_cluster;i++){
			cluster[i]=new LinkedList<>();
		}
		this.cendroid_converge();//appel de FCMT2
		
		alldataset = new ArrayList<DataSet>();
		
		for(int i = 0; i < fUsedDatacenters.size(); i++)
		{
			DataCenter thisCenter = fUsedDatacenters.get(i);
			for(int j = 0; j < thisCenter.getDatasets().size(); j++)
				alldataset.add(thisCenter.getDatasets().get(j));
		}
	
		//return the best solution with fuzzy
		if(compt!=num_cluster){
			//return the first partition without 
			System.out.println("--the first solution---");
			fuzzyUsedDatacenters = fUsedDatacenters;
		}
		else
		{
			System.out.println("i am here+++++++++++++++++++++++");
			fuzzyUsedDatacenters = fUsedDatacenters;
			
			//clear all datacenters --> reset
			for(int j=0;j<num_cluster;j++)
				fuzzyUsedDatacenters.get(j).getDatasets().clear();
			
			//add datasets
			for(int i=0;i<essaie_number;i++){
				int max=0;
				double value=0;
				for(int j=0;j<num_cluster;j++){
					if(lu[i][j]>value){
						max=j;
						value=lu[i][j];
					}
				}
				cluster[max].add(data_point.get(i));//a deplacer dans centroid_converge avant return
				System.out.println("---/////max="+max+" cluster[max]="+data_point.get(i).toString3());
				//max+1 contains the best datacenter for the i dataset
				//exception to add here (dc2 cannot add d1, 74.0 required, 12.0 available)
				fuzzyUsedDatacenters.get(max).addDataset(alldataset.get(i));
			}	
		}
	}
	
	public ArrayList<DataCenter> getFuzzyDC(){
		return fuzzyUsedDatacenters;
	}
	
	//initialisation phase (donner des poids de moy au centres vectors)
	private void Fuzzy() 
	{
		matrix=new double[essaie_number][num_cluster];
		matrix2=new double[essaie_number][num_cluster];
		lower=new double[essaie_number][num_cluster];
		upper=new double[essaie_number][num_cluster];
		lu=new double[essaie_number][num_cluster];
		this.data_point=new LinkedList<>();
		this.cluster_point=new LinkedList<>();
		this.cluster_point2=new LinkedList<>();
		
		//init des points par les datasets
		for(int i=0;i<essaie_number;i++){
			String name =mat.getDatasets().get(i).getName();
			Double size =mat.getDatasets().get(i).getSize();
			double dep =mat.dep(mat, i, i);
		
			FuzzyPoint dataset=new FuzzyPoint(dep,name,size);
			this.data_point.add(dataset);
			
		}
		
		int start=0; int end = 0;
		//init des centers vectors par la moy des dependences de chaque centre (cluster ou datacenter)
		for(int i=0;i<num_cluster;i++)
		{
			double center = 0;
			int n_dataset = fUsedDatacenters.get(i).getDatasets().size();
			end = end + n_dataset;
			for(int j=start;j<end;j++){
				center = center +mat.dep(mat, j, j);
			}
			start=end; 
			center = center /n_dataset;
			String nameDC = fUsedDatacenters.get(i).getName();
			Double sizeDC = dc_size - fUsedDatacenters.get(i).freeSpace();
			FuzzyCenter data = new FuzzyCenter(center, nameDC, sizeDC);
			this.cluster_point.add(data);//ajouter les datasets au centres!!
			this.cluster_point2.add(data);//ajouter les datasets au centres(type2)
		}
	}
	
	//initialise fuzzy partition matrix (1ere etpae)
	public void init()
	{
		for(int i=0;i<essaie_number;i++){
			System.out.println();
			for(int j=0;j<num_cluster;j++){
				matrix[i][j]=dependency_diff(data_point.get(i).dep, cluster_point.get(j).dep);
				matrix2[i][j] = dependency_diff(data_point.get(i).dep, cluster_point2.get(j).dep);
				System.out.print("|"+matrix[i][j]+" ");
			}
		}
	}
	public double dependency_diff(double x1, double x2){
		return (Math.abs(x1-x2));
	}
	
	public void membership_matrix()
	{
		for(int i=0;i<essaie_number;i++){
			System.out.println();
			double result=0.0,result2=0.0;
			for(int j=0;j<num_cluster;j++){
				result=result+Math.pow(1/matrix[i][j],2/(m1-1));
				result2=result2+Math.pow(1/matrix2[i][j],2/(m2-1));
			}
			for(int j=0;j<num_cluster;j++){
				matrix[i][j]=Fuzzy_function.floorconvert(Math.pow(1/matrix[i][j],2/(m1-1))/result);
				matrix2[i][j]=Fuzzy_function.floorconvert(Math.pow(1/matrix2[i][j],2/(m2-1))/result2);
				
				lower[i][j]=Math.min(matrix[i][j], matrix2[i][j]);
				upper[i][j]=Math.max(matrix[i][j], matrix2[i][j]);
                                upper[i][j]=0.75*(Math.abs(matrix[i][j]- matrix2[i][j])/(matrix[i][j]+matrix2[i][j])+Math.abs(matrix2[i][j]-matrix[i][j])/(2-matrix[i][j]- matrix2[i][j]));
				lower[i][j]=0.45*Math.max(matrix[i][j], matrix2[i][j]);
				lu[i][j]=(lower[i][j]+upper[i][j])/2;
				System.out.print("|"+lu[i][j]+" ");
			}
		}
		System.out.println();
	}
	
	public List<FuzzyCenter> new_cendroid()
	{
		List<FuzzyCenter> center_point=new LinkedList<>();
		List<FuzzyCenter> center_point2=new LinkedList<>();
		List<FuzzyCenter> center_reduce=new LinkedList<>();
		for(int j=0;j<num_cluster;j++)
		{
			double result=0.0,x=0.0;
			double result2=0.0,x2=0.0;
			
			for(int i=0;i<essaie_number;i++){
				result=result+Math.pow(matrix[i][j],m1);
				result2=result2+Math.pow(matrix[i][j],m2);
			}
			for(int i=0;i<essaie_number;i++){
				x=x+Math.pow(matrix[i][j],m1)*data_point.get(i).dep;
				x2=x2+Math.pow(matrix2[i][j],m2)*data_point.get(i).dep;//a verifier
			}

			center_point.add(new FuzzyCenter(x/result,cluster_point.get(j).name,0.0));
			center_point2.add(new FuzzyCenter(x2/result2,cluster_point2.get(j).name,0.0));
			center_reduce.add(new FuzzyCenter(((x/result)+(x2/result2))/2,cluster_point.get(j).name,0.0));
			System.out.println("Cluster(DC) "+j+"    " +" X/R="+ ((x/result)+(x2/result2))/2);
		}
		return center_reduce;
	}

	public void cendroid_converge(){
		boolean cond=true;int ind=1;
		System.out.println("---------------------------------------------------------- ");
		init();
		loops:
		while(cond)
		{	
			System.out.println("FCM Matrix");
			//init();
			System.out.println("\n");
			System.out.println("\nCalling Membership function (lower+upper)/2");
			membership_matrix();
			System.out.println("-----------New centroid---N° "+ind); ind++;
			//un tour avec tous les clusters(DC)
			List<FuzzyCenter> center=new_cendroid();
			//verif stockage
			center = cluster_space(center);
			
			for(int j=0;j<num_cluster;j++){
				Double x1=center.get(j).dep;

				if(x1.isNaN()){
					break loops;
				}
				if(center.get(j).size<=dc_size){
					compt++;
					if((center.get(j).dep - cluster_point.get(j).dep<epsilon)){
						cond=false;
						System.out.println("diff(<eps)="+(center.get(j).dep - cluster_point.get(j).dep));
					}
					else{
						System.out.println("diff(>=eps)="+(center.get(j).dep - cluster_point.get(j).dep));
						cond=true;
						//condition local optimum
						if(ind==25){
							cond=false;
							this.cluster_point = center;
						}
					}
				}
				else{
					System.out.println("size>dc_size");
					cond=false;
				}
			}
		/*	if(compt == num_cluster)//centroid_valide//all DC_size is valid
			{
				//recover the best solution
				for(int i=0;i<essaie_number;i++){
					int max=0;
					double value=0;
					for(int j=0;j<num_cluster;j++){
						if(matrix[i][j]>value){
							max=j;
							value=matrix[i][j];
						}
					}
					cluster[max].add(data_point.get(i));//a deplacer dans centroid_converge avant return
				}
				this.cluster_point = center;
			}*/
		}
	}
	
	public List<FuzzyCenter> cluster_space(List<FuzzyCenter> fc){
		
		for(int i=0;i<essaie_number;i++){
			int max=0;
			double value=0;
			for(int j=0;j<num_cluster;j++){
				if(lu[i][j]>value){
					max=j;
					value=lu[i][j];
				}
			}
			fc.get(max).setSize(fc.get(max).size + data_point.get(i).size);
			//cluster[max].add(data_point.get(i));//a deplacer dans centroid_converge avant return
			//fc.get(max).setDataPoint(data_point.get(i));
			
		}
		for(int j=0;j<num_cluster;j++){
			System.out.println(fc.get(j).toString3());
		}	
		return fc;
	}
}
