package dataPlacement;

import java.util.ArrayList;

import workflow.Workflow;

public class Matrix
{
	private ArrayList<DataSet> fDatasets;
	private int[][] fData;
	
	public Matrix()
	{
		fDatasets = new ArrayList<DataSet>();
		fData = new int[0][0];
	}
	
	public Matrix(Workflow aWorkflow)
	{
		this();
		for(int i = 0; i < aWorkflow.getDatasets().size(); i++)
			if(aWorkflow.getDatasets().get(i).exists())
				addDataset(aWorkflow.getDatasets().get(i));
	}
	
	public int[][] getData()
	{
		return fData;
	}
	
	public double[][] getDataDouble(int[][] data)
	{
		double [][] mat;
		mat = new double [0][0];
		for(int i=0;i<data.length;i++)
		{
			for(int j=0;j<data.length;j++)
			{
				mat[i][j]=data[i][j];
			}
		}
		return mat;
	}
	
	public ArrayList<DataSet> getDatasets()
	{
		return fDatasets;
	}
	
	public void addDataset(DataSet aDataset)
	{
		if(fDatasets.contains(aDataset))
			return;
		
		//create a new row in fData
		addRowAndColumn();
		
		//calculate the dependancy between aDataset and each in fDatasets
		for(int i = 0; i < fData.length - 1; i++)
		{
			int lDependancy = calculateDependancy(aDataset, fDatasets.get(i));
			
			fData[i][fData.length - 1] = lDependancy;
			fData[fData.length - 1][i] = lDependancy;
		}
		
		//fill in the bottom right cell
		fData[fData.length - 1][fData.length - 1] = aDataset.getTasks().size();
		
		//add aDataset to fDatasets
		fDatasets.add(aDataset);
	}
	
	//Adds a row and a column to the matrix array
	//WARNING: This operation takes LINEAR TIME! as the size of the arrays increases
	//			so does the time that this operation will take!
	private void addRowAndColumn()
	{
		int lNewSize = fData.length + 1;
		int[][] lNewMatrix = new int[lNewSize][lNewSize];
		
		for(int i = 0; i < lNewSize - 1; i++)
		{
			for(int j = 0; j < lNewSize - 1; j++)
			{
				lNewMatrix[i][j] = fData[i][j];
			}
		}
		
		fData = lNewMatrix;
	}
	
	public static int calculateDependancy(DataSet aDataset1, DataSet aDataset2)
	{
		int lCount = 0;
		
		ArrayList<Task> ds1Tasks = aDataset1.getTasks(), ds2Tasks = aDataset2.getTasks(); 
		
		for(int i = 0; i < ds1Tasks.size(); i++)
		{
			if(ds2Tasks.contains(ds1Tasks.get(i)))
				lCount++;
		}
		
		return lCount;
	}
	
	//fuzzy c-means calculate dependancy
	public static int calculateDependancyFuzzy(DataSet aDataset1, DataSet aDataset2)
	{
		int lCount = 0;
		
		ArrayList<Task> ds1Tasks = aDataset1.getTasks(), ds2Tasks = aDataset2.getTasks(); 
		
		for(int i = 0; i < ds1Tasks.size(); i++)
		{
			if(ds2Tasks.contains(ds1Tasks.get(i)))
				lCount++;
		}
		
		return lCount;
	}
	
	public String toString()
	{
		String result = "";
		for(int i = 0 ; i < fData.length; i++)
		{
			for(int j = 0; j < fData[i].length; j++)
			{
				result += fData[i][j] + " ";
			}
			
			result += "\r\n";
		}
		
		return result;
	}
	public String toString2(Matrix a,int start,int end)
	{
		String result = "";
		for(int i = start ; i < end; i++)
		{
			for(int j = 0; j < end; j++)
			{
				result += fData[i][j] + " ";
			}
			
			result += "\r\n";
		}
		
		return result;
	}
	
	public int dep (Matrix a,int start,int end)
	{
		return a.fData[start][end];		
	}
}
