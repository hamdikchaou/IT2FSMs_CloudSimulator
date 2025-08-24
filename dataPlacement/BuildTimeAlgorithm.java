package dataPlacement;

import java.util.ArrayList;
import java.util.Random;

import workflow.Workflow;

public abstract class BuildTimeAlgorithm
{
	protected ArrayList<DataCenter> fDataCenters;
	protected Random fRandom;
	
	public BuildTimeAlgorithm(ArrayList<DataCenter> aDataCenters)
	{
		fDataCenters = aDataCenters;
		fRandom = new Random();
	}
	
	public abstract ArrayList<DataCenter> distribute(Workflow aWorkflow) throws DistributionException;
	
	public abstract ArrayList<DataCenter> distribute(ArrayList<DataSet> aDatasets) throws DistributionException;
	
	public abstract void setDependancyMatrix(Matrix aMatrix);
	
	public abstract Matrix getDependancyMatrix();
	
	public abstract Matrix getDependancyMatrixClustered();
	
	public abstract ArrayList<DataCenter> distributeFuzzy(Matrix fuzzyMatrix);
}
