package dataPlacement;

//import org.wsrd.generatingWSRDFile.HardWSRDFile;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import fuzzy_param.Fuzzy_function;
import fuzzy_param.Fuzzy_parametre;
import dataPlacement.FuzzyCenter;
import dataPlacement.FuzzyPoint;

public class FcmPartition {
	private static int essaie_number;//number of datasets
	private static int num_cluster;//number of datacenters
	private static double fuzziness=2.4;
	private static double after_dot=3;
	private static double epsilon=0.001;

	private double[][] matrix;
	private Matrix mat;
	private double dc_size;
	int compt=0;
	
	private ArrayList<DataCenter> fUsedDatacenters;
	private ArrayList<DataCenter> fuzzyUsedDatacenters;
	
	private ArrayList<DataSet> alldataset;
	
	private List<FuzzyPoint> data_point;
	public List<FuzzyCenter> cluster_point;
	
	private List<FuzzyPoint>[] cluster;
	
	
	public FcmPartition(ArrayList<DataCenter> fDatacenters,Matrix mat) throws Exception
	{
		this.fUsedDatacenters = fDatacenters;
		this.num_cluster = fUsedDatacenters.size();
		this.mat=mat;
		this.matrix = new double[essaie_number][num_cluster];
		essaie_number=mat.getData().length;
		this.dc_size=fDatacenters.get(0).getSize()*fDatacenters.get(0).getP_ini();
		
		this.Fuzzy();//phase d'initialisation
		System.out.println("---------------Init_fuzzy_before_membership-------------");
		for(int j=0;j<num_cluster;j++){
			//System.out.println(cluster_point.get(j).toString3());
		}
		//System.out.println("");
		
		cluster=new List[num_cluster];
		for(int i=0;i<num_cluster;i++){
			cluster[i]=new LinkedList<>();
		}
		this.cendroid_converge();
		
		alldataset = new ArrayList<DataSet>();
		
		for(int i = 0; i < fUsedDatacenters.size(); i++)
		{
			DataCenter thisCenter = fUsedDatacenters.get(i);
			//result += thisCenter.getName() + " : ";
			for(int j = 0; j < thisCenter.getDatasets().size(); j++)
			{
				//esult += thisCenter.getDatasets().get(j).getName() + " (" + thisCenter.getDatasets().get(j).getSize() + "); ";
				alldataset.add(thisCenter.getDatasets().get(j));
			}
		}
	/*	for(int i = 0; i < essaie_number; i++)
			System.out.println("ici-------> "+alldataset.get(i).toString());
	/*	for(int i=0;i<num_cluster;i++){
			
			alldataset.add(mat.getDatasets().get(i));
			fuzzyUsedDatacenters.get(j).resetDataCenter();
		}*/
		
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
			for(int j=0;j<num_cluster;j++){
				//fuzzyUsedDatacenters.get(j).resetDataCenter();
				fuzzyUsedDatacenters.get(j).getDatasets().clear();
			}
			
			//add datasets
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
				System.out.println("---/////max="+max+" cluster[max]="+data_point.get(i).toString3());
				//max+1 contains the best datacenter for the i dataset
				//exception to add here (dc2 cannot add d1, 74.0 required, 12.0 available)
				fuzzyUsedDatacenters.get(max).addDataset(alldataset.get(i));
			}
			
		/*	for(int j=0;j<num_cluster;j++){
				for(int i=0;i<essaie_number;i++){
					if (cluster[j].get(i)!=null)
					System.out.println(cluster[j].get(i).toString3()); 
				}
			}*/
		}
		
		//affichage des datasets
	/*	for(int i=0;i<essaie_number;i++){
			System.out.println(data_point.get(i).toString3());
		}*/
			
//		List<FuzzyPoint>[] clust =this.cluster();			
	}
	
	public ArrayList<DataCenter> getFuzzyDC(){
		return fuzzyUsedDatacenters;
	}
	
	//initialisation phase
	private void Fuzzy() 
	{
		matrix=new double[essaie_number][num_cluster];

		this.data_point=new LinkedList<>();
		this.cluster_point=new LinkedList<>();
		
		//init des points par les datasets
		for(int i=0;i<essaie_number;i++){
			String name =mat.getDatasets().get(i).getName();
			Double size =mat.getDatasets().get(i).getSize();
		//	DataCenter center =mat.getDatasets().get(i).getDC();
			double dep =mat.dep(mat, i, i);
		
			FuzzyPoint dataset=new FuzzyPoint(dep,name,size);
			this.data_point.add(dataset);
			
		}
		
		 int start=0; int end = 0;
		//init des centers vectors par la moy des dependences de chaque centre
		for(int i=0;i<num_cluster;i++)
		{
			double center = 0;
			int n_dataset = fUsedDatacenters.get(i).getDatasets().size();
			end = end + n_dataset;
			for(int j=start;j<end;j++){
				center = center +mat.dep(mat, j, j);//System.out.println("dep:"+mat.dep(mat, j, j));
				//alldataset.add(fUsedDatacenters.get(j).getDatasets());
			}
			start=end; 
			center = center /n_dataset;
			String nameDC = fUsedDatacenters.get(i).getName();
			Double sizeDC = dc_size - fUsedDatacenters.get(i).freeSpace();
			FuzzyCenter data = new FuzzyCenter(center, nameDC, sizeDC);
			this.cluster_point.add(data);//System.out.println(center+nameDC+sizeDC);
			//ajouter les datasets au centres!!
		}
	}
	
	public void init()
	{
		for(int i=0;i<essaie_number;i++){
			//System.out.println();
			for(int j=0;j<num_cluster;j++){
				matrix[i][j]=dependency_diff(data_point.get(i).dep, cluster_point.get(j).dep);
		//		System.out.print(" |"+data_point.get(i).dep+" C:"+cluster_point.get(j).dep);
				//System.out.print("|"+matrix[i][j]+" ");
			}
		}
	}
	public double dependency_diff(double x1, double x2){
		return x1-x2;
	}
	
	public void membership_matrix()
	{
		for(int i=0;i<essaie_number;i++){
			//System.out.println();
			double result=0.0;
			for(int j=0;j<num_cluster;j++){
				result=result+Math.pow(1/matrix[i][j],2/(fuzziness-1));
				//matrix[i][j] = 1.0 / Math.pow(matrix[i][j] / result, 2.0 / (fuzziness - 1.0));
				//result += matrix[i][j];
			}
			for(int j=0;j<num_cluster;j++){
				matrix[i][j]=Fuzzy_function.floorconvert(Math.pow(1/matrix[i][j],2/(fuzziness-1))/result);
				//matrix[i][j]=matrix[i][j]/result;
				//System.out.print("|"+matrix[i][j]+" ");
			}
		}
		//System.out.println();
	}
	
	public List<FuzzyCenter> new_cendroid()
	{
		List<FuzzyCenter> center_point=new LinkedList<>();
		for(int j=0;j<num_cluster;j++)
		{
			double result=0.0,x=0.0;
			for(int i=0;i<essaie_number;i++){
				result=result+Math.pow(matrix[i][j],fuzziness);
			}
			for(int i=0;i<essaie_number;i++){
				x=x+Math.pow(matrix[i][j],fuzziness)*data_point.get(i).dep;
			}

			center_point.add(new FuzzyCenter(x/result,cluster_point.get(j).name,0.0));
			//System.out.println(center_point.get(j).toString3());
			//System.out.println(j+"    "+" X="+x+"   "+" result="+result+"    "+" X/R="+ x/result);
			System.out.println("Cluster(DC) "+j+"    " +" X/R="+ x/result);
		}
		return center_point;
	}

	public void cendroid_converge(){
		boolean cond=true;int ind=1;
		System.out.println("---------------------------------------------------------- ");
		//init();
		loops:
		while(cond)
		{
			System.out.println("FCM Matrix");
			init();
			System.out.println("\n");
			System.out.println("\nCalling Membership function");
			membership_matrix();
			System.out.println("-----------New centroid---N° "+ind); ind++;
			//un tour avec tous les clusters(DC)
			List<FuzzyCenter> center=new_cendroid();
			//verif stockage
			center = cluster_space(center);
			//int compt=0;
			
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
						if(ind==100){
							cond=false;
							this.cluster_point = center;
						}
					}
				}
				else{
					System.out.println("size>dc_size");
					cond=false;
				}
			/*	if((center.get(j).dep - cluster_point.get(j).dep<epsilon)&&(center.get(j).size>dc_size)){
					//&&(center.get(j))
					cond=false;
					System.out.println("diff(<eps)="+(center.get(j).dep - cluster_point.get(j).dep));
				}
				else{
					System.out.println("diff(>=eps)="+(center.get(j).dep - cluster_point.get(j).dep));
					cond=true;
				}*/
			}
			if(compt == num_cluster)//centroid_valide//all DC_size is valid
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
			}
			//this.cluster_point = center;
		}
		//List<FuzzyPoint>[] clust =this.cluster();
	}
	
	public List<FuzzyCenter> cluster_space(List<FuzzyCenter> fc){
		
		for(int i=0;i<essaie_number;i++){
			int max=0;
			double value=0;
			for(int j=0;j<num_cluster;j++){
				if(matrix[i][j]>value){
					max=j;
					value=matrix[i][j];
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
	
/*	public List<FuzzyPoint>[] cluster()
	{
		List<FuzzyPoint>[] cluster=new List[num_cluster];
		double size =0;
		for(int i=0;i<num_cluster;i++){
			cluster[i]=new LinkedList<>();
		}
		for(int i=0;i<essaie_number;i++){
			int max=0;
			double value=0;
			for(int j=0;j<num_cluster;j++){
			//	System.out.println("before if_max="+max+" value="+value);
				if(matrix[i][j]>value){
					max=j;
					value=matrix[i][j];
				}
			//	System.out.println("matrix[i][j]="+matrix[i][j]);
			//	System.out.println("max="+max+" value="+value);
			}
			//System.out.println("new");
			cluster[max].add(data_point.get(i));
			System.out.println("---/////max="+max+" cluster[max]="+data_point.get(i).toString3());
		}
/*		for(int l=0;l<num_cluster;l++){
			if(cluster[l].get(l)!=null)
			{
				System.out.println("---l="+l+"="+cluster[l].get(l).dep);
			}
		}
		return cluster;
	}	*/
	
}
