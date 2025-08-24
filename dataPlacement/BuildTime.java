package dataPlacement;

import java.util.ArrayList;

import fuzzy.Fuzzy;
import workflow.Workflow;

public class BuildTime extends BuildTimeAlgorithm
{
	private Matrix fDependancyMatrix;
	private Matrix fClusteredMatrix;
	private Clusterer fClusterer = new BEA();
	private Fuzzy fcm;
	
	public BuildTime(ArrayList<DataCenter> aDatacenters)
	{
		super(aDatacenters);
	}
	
	public ArrayList<DataCenter> getDatacenters()
	{
		return fDataCenters;
	}
	public void setDatacenters(ArrayList<DataCenter> aDatacenters)
	{
		fDataCenters = aDatacenters;
	}
	public Matrix getDependancyMatrix()
	{
		return fDependancyMatrix;
	}
	public Matrix getDependancyMatrixClustered()
	{
		return fClusteredMatrix;
	}
	public void setDependancyMatrix(Matrix aDependancyMatrix)
	{
		fDependancyMatrix = aDependancyMatrix;
	}
	
	public ArrayList<DataCenter> distributeFuzzy(Matrix fuzzyMatrix)
	{
		ArrayList<DataCenter> result = new ArrayList<DataCenter>();
		return result;
	}
	
	public ArrayList<DataCenter> distribute(Workflow aWorkflow) throws DistributionException
	{
		System.out.println("-->Distribution_from BuildTime Class");
		
		fDependancyMatrix = new Matrix(aWorkflow);
		//System.out.println("Matrix dependencies");
		//System.out.println(fDependancyMatrix.toString());
		try
		{
			return distribute();
		}
		catch (Exception ex)
		{
			throw new DistributionException(ex.getMessage());
//			System.out.println("ERROR: " + ex.getMessage());
//			System.exit(-1);
//			return null;
		}
	}
	
	public ArrayList<DataCenter> distribute(ArrayList<DataSet> aDatasets) throws DistributionException
	{
		fDependancyMatrix = new Matrix();
		for(int i = 0; i < aDatasets.size(); i++)
		{
			fDependancyMatrix.addDataset(aDatasets.get(i));
		}
		try
		{
			return distribute();
		}
		catch (Exception ex)
		{
            throw new DistributionException(ex.getMessage());
//			System.out.println("ERROR: " + ex.getMessage());
//			System.exit(-1);
//			return null;
		}
	}
	
	private ArrayList<DataCenter> distribute() throws Exception
	{
		/* perform the distribution of datasets (from the fDependancyMatrix)
		 * to the datacenters in fDataCenters
		 *  fClusterer.cluster() will need to be invoked to determine the grouping.
		 */		
		fClusteredMatrix = fClusterer.cluster(fDependancyMatrix);
		//System.out.println("Matrix dependencies_clustered");
		//System.out.println(fClusteredMatrix.toString());
		
		//blank out the datacenters
		for(int i = 0; i < fDataCenters.size(); i++)
			fDataCenters.get(i).getDatasets().clear();
		
		//partition and assign the clustered matrix over the datacenters
		// a copy is made so that items can be removed from the list of datacenters
		// without affecting fDataCenters
		partitionAndAssign(fClusteredMatrix, new ArrayList<DataCenter>(fDataCenters));
	//	partitionAndAssignFuzzy(fClusteredMatrix, new ArrayList<DataCenter>(fDataCenters));
		
		//return only the ones that are used!
		ArrayList<DataCenter> result = new ArrayList<DataCenter>();
		
		for(int i = 0; i < fDataCenters.size(); i++)
			if(fDataCenters.get(i).getDatasets().size() > 0)
				result.add(fDataCenters.get(i));
		
		
		return result;
	}
	
	private void partitionAndAssignFuzzy(Matrix aMatrix, ArrayList<DataCenter> aDatacenters)throws Exception
	{
		if(aMatrix.getDatasets().size() == 0)
			throw new Exception("Can't repartition an empty matrix!");
		
		fcm = new Fuzzy();
		//mats contains the clustered dependency matrix (int)
		int [][] mats = aMatrix.getData();
		double [][] mat = new double [mats.length][mats.length] ;
		
		for(int i=0;i<mats.length;i++){
			for(int j=0;j<mats.length;j++){
				mat[i][j] = mats[i][j];
			}
		}
		//mat contains the clustered dependency matrix (double)
		fcm.setMatrix(mat);
	}
	
    //methode responsable du partitionnement des datasets par la recherche du meilleur PM
	private void partitionAndAssign(Matrix aMatrix, ArrayList<DataCenter> aDatacenters) throws Exception
	{
		if(aMatrix.getDatasets().size() == 0)
			throw new Exception("Can't repartition an empty matrix!");
		
		//find a good partition point
		int bestPoint = 0;
		int bestPointScore = 0;
		
		//try all the possible points to find the best one
		for(int i = 1; i < aMatrix.getDatasets().size(); i++)
		{
			int thisPointScore = calculatePartitionPointScore(aMatrix, i);

			if(thisPointScore > bestPointScore)
			{
				bestPointScore = thisPointScore;
				bestPoint = i;
			}
		}
		System.out.println("-->best point= "+bestPoint);
		
		//assign the left Datasets
		assignDatasets(aMatrix, aDatacenters, 0, bestPoint);
		
		//assign the right Datasets
		assignDatasets(aMatrix, aDatacenters, bestPoint + 1, aMatrix.getDatasets().size() - 1);
	}
	
	private void assignDatasets(Matrix aMatrix, ArrayList<DataCenter> aDatacenters, int startPoint, int endPoint) 
			throws Exception
	{
		double lDataSize = 0;
	//	System.out.println("Sub_matrix ");
	//	System.out.println(aMatrix.toString2(aMatrix, startPoint+1, endPoint));
		for(int i = startPoint; i <= endPoint; i++){
			
			lDataSize += aMatrix.getDatasets().get(i).getSize();
			//System.out.println("Dataset N� "+aMatrix.getDatasets().get(i)+"="+aMatrix.getDatasets().get(i).getSize());
		}
		//System.out.println("-->Sum_sub_matrix "+lDataSize);
		DataCenter lBestCenter = getBestDataCenter(lDataSize, new ArrayList<DataCenter>(fDataCenters));
		
		if(lBestCenter == null)
		{
			if(aMatrix.getDatasets().size() == 1)
			{
                for(DataCenter d : aDatacenters)
                    System.err.println(d.getName() + d.freeSpace());
                System.err.println(aDatacenters.size() + " "+ fDataCenters.size());
				throw new DistributionException("Can't assign dataset " + aMatrix.getDatasets().get(0) + " " + lDataSize
						+ " to any datacenter");
			}
			
			//create a new matrix out of this partition
			Matrix lMatrix = new Matrix();
			
			for(int i = startPoint; i <= endPoint; i++)
				lMatrix.addDataset(aMatrix.getDatasets().get(i));
			
			partitionAndAssign(lMatrix, aDatacenters);
		}
		else
		{
			//System.out.println("subMatrixDatasets_fit_to DC�="+lBestCenter);
			//if this partition will fit into one center, assign them all!
			for(int i = startPoint; i <= endPoint; i++)
			{
				lBestCenter.addDataset(aMatrix.getDatasets().get(i));
				aMatrix.getDatasets().get(i).setDC(lBestCenter);
			}
			
			//remove this datacenter from the list of available ones
			aDatacenters.remove(lBestCenter);
		}
	}

	private void assignLeftDatasets(Matrix aMatrix, ArrayList<DataCenter> aDatacenters, int partitionPoint) 
			throws Exception
	{
		double leftDataSize = 0;
		for(int i = 0; i <= partitionPoint; i++)
			leftDataSize += aMatrix.getDatasets().get(i).getSize();
		
		DataCenter lLeftDatacenter = getBestDataCenter(leftDataSize, new ArrayList<DataCenter>(fDataCenters));
		
		//if it doesn't fit into any, re-partition!
		if(lLeftDatacenter == null) 
		{
			//if there is only one dataset and it still doesn't fit, we can't repartition!
			if(aMatrix.getDatasets().size() == 1)
				
				throw new Exception("Can't assign dataset " + aMatrix.getDatasets().get(0)
									+ " to any datacenter!");	
				
			//create a new matrix with only the ones from the left
			Matrix lLeftMatrix = new Matrix();
			
			for(int i = 0; i <= partitionPoint; i++)
				lLeftMatrix.addDataset(aMatrix.getDatasets().get(i));
			
			partitionAndAssign(lLeftMatrix, aDatacenters);
		}
		else
		{
			//if it does, assign the datasets into the best center
			for(int i = 0; i <= partitionPoint; i++)
			{
				try
				{
					lLeftDatacenter.addDataset(aMatrix.getDatasets().get(i));
					aMatrix.getDatasets().get(i).setDC(lLeftDatacenter);
				}
				catch (Exception ex)
				{
					//size issue! handle it here!
					System.out.println("ERROR: " + ex.getMessage());
				}
			}
			//remove this datacenter from the list of available ones
			aDatacenters.remove(lLeftDatacenter);
		}
	}
	
	private void assignRightDatasets(Matrix aMatrix, ArrayList<DataCenter> aDatacenters, int partitionPoint) throws Exception
	{
		double rightDataSize = 0;
		for(int i = partitionPoint + 1; i < aMatrix.getDatasets().size(); i++)
			rightDataSize += aMatrix.getDatasets().get(i).getSize();
		
		DataCenter lRightDatacenter = getBestDataCenter(rightDataSize, new ArrayList<DataCenter>(fDataCenters));
		
		//if it doesn't fit into any, re-partition!
		if(lRightDatacenter == null) 
		{
			if(aMatrix.getDatasets().size() == 1)
				throw new Exception("Can't assign dataset " + aMatrix.getDatasets().get(0)
									+ " to any datacenter!");
			
			//create a new matrix with only the ones from the right
			Matrix lRightMatrix = new Matrix();
			
			for(int i = partitionPoint + 1; i < aMatrix.getDatasets().size(); i++)
				lRightMatrix.addDataset(aMatrix.getDatasets().get(i));
			
			partitionAndAssign(lRightMatrix, aDatacenters);
		}
		else
		{
			//if it does, assign the datasets into the best center
			for(int i = partitionPoint + 1; i < aMatrix.getDatasets().size(); i++)
			{
				try
				{
					lRightDatacenter.addDataset(aMatrix.getDatasets().get(i));
					aMatrix.getDatasets().get(i).setDC(lRightDatacenter);
				}
				catch (Exception ex)
				{
					//size issue! handle it here!
					System.out.println("ERROR: " + ex.getMessage());
				}
			}
			
			//remove this datacenter from the list of available ones
			aDatacenters.remove(lRightDatacenter);
		}
	}
	
	//calculte the best point partition PM
	private int calculatePartitionPointScore(Matrix aMatrix, int aPoint)
	{
		int topLeft = 0;
		
		for(int i = 0; i <= aPoint; i++)
			for(int j = 0; j <= aPoint; j++)
				topLeft += aMatrix.getData()[i][j];
		
		int bottomRight = 0;
		for(int i = aPoint + 1; i < aMatrix.getData().length; i++)
			for(int j = aPoint + 1; j < aMatrix.getData().length; j++)
				bottomRight += aMatrix.getData()[i][j];
		
		int excludedPoints = 0;
		for(int i = 0; i <= aPoint; i++)
			for(int j = aPoint + 1; j < aMatrix.getData().length; j++)
				excludedPoints += aMatrix.getData()[i][j];
		
		int result = topLeft * bottomRight;
		
		result -= Math.pow(excludedPoints, 2);
		
		return result;
	}
	
    //calculate the suitable datacenter for affecting datasets_sub matrix
	private DataCenter getBestDataCenter(double aDatasize, ArrayList<DataCenter> aDatacenters)
	{
		DataCenter result = null;
		
		for(int i = 0; i < aDatacenters.size(); i++)
		{
			double thisCentersPotential = aDatacenters.get(i).freeSpace();
			
			//can this center handle this bunch of data?
			if(thisCentersPotential < aDatasize)
				continue;
			
			//is this the first that can fit it or
			if(result == null)
				result = aDatacenters.get(i);
			//is this center a better fit for this bunch of data?
			else if(thisCentersPotential < (result.getP_ini() * result.getSize()))
				result = aDatacenters.get(i);
		}
		if (result!=null)
			System.out.println("Free space "+result.freeSpace()+" at DC�="+result+"\n");
		
		return result;
	}
}
