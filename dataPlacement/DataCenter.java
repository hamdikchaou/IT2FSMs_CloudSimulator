package dataPlacement;

import java.util.ArrayList;

public class DataCenter
{
	private String fName;
	private double fSize;
	private double fP_ini, fP_max;
	//private double fLoad;
	private ArrayList<DataSet> fDatasets;
	
	private int fTaskExecutionCount;
	private int fDataSetMovementCount;
	
	private boolean isAtMaxCapacity = false;
    private boolean hasFixedData;
	
	public DataCenter()
	{
		fDatasets = new ArrayList<DataSet>();
		fTaskExecutionCount = 0;
		fDataSetMovementCount = 0;
	}

	public int getTaskExecutionCount()
	{
		return fTaskExecutionCount;
	}
	
	public int getDatasetMovementCount()
	{
		return fDataSetMovementCount;
	}
	
	public void resetDataCenter()
	{
		//clear all datasets
		fDatasets.clear();
		isAtMaxCapacity = false;
		fDataSetMovementCount = 0;
		fTaskExecutionCount = 0;
	}
	
	public void resetDataCenterCounts()
	{
		fDataSetMovementCount = 0;
		fTaskExecutionCount = 0;
	}
	
	public void setMaxCapacity(boolean aValue)
	{
		isAtMaxCapacity = aValue;
	}
	
	public ArrayList<DataSet> execute(Task aTask) throws Exception
	{
                //More code may need to go here!
		if(canExecute(aTask))
		{
			System.out.println("Datacenter " + fName + " is executing " + aTask.getName()+"="+aTask.getInput() + "...");
		}
		else
		{
			ArrayList<DataSet> lRetrieved = findRetrievedSets(aTask);
			System.out.print("Datacenter " + fName + " is executing " + aTask.getName()+"="+aTask.getInput()+ " by retrieving ");
			for(int i = 0; i < lRetrieved.size(); i++)
			{
				System.out.print(lRetrieved.get(i) + "; ");
			}
			System.out.println();
		}
		
		for(int i = 0; i < aTask.getOutput().size(); i++)
		{
			aTask.getOutput().get(i).setExists(true);
			//aTask.getOutput().get(i).setDC(this);
			//fDatasets.add(aTask.getOutput().get(i));
		}
		fTaskExecutionCount++;
		return aTask.getOutput();
	}
	
	private ArrayList<DataSet> findRetrievedSets(Task aTask)
	{
		ArrayList<DataSet> result = new ArrayList<DataSet>();
		
		for(int i = 0; i < aTask.getInput().size(); i++)
			if(!fDatasets.contains(aTask.getInput().get(i)))
				result.add(aTask.getInput().get(i));
		
		return result;
	}
	
	public boolean canExecute(Task aTask)
	{
		//check that this datacenter has all the data required to execute the task
		if(!checkDependancies(aTask))
			return false;
		
		return true;
	}
	
	private boolean checkDependancies(Task aTask)
	{
		for(int i = 0; i < aTask.getInput().size(); i++)
			if(!fDatasets.contains(aTask.getInput().get(i)))
				return false;
		
		return true;
	}
	
	public ArrayList<DataSet> getDatasets()
	{
		return fDatasets;
	}
	
	public void addDataset(DataSet aDataset) throws Exception
	{
        if(fDatasets.contains(aDataset))
            return;
		if(!aDataset.exists())
		{
			//something wierd is happening!
			System.out.println("Non existant dataset " + aDataset + " was added to " + this);
			return;
		}
		
		if(freeSpace() >= aDataset.getSize())
		{
			fDatasets.add(aDataset);
			fDataSetMovementCount++;
		}
		else
		{
			throw new Exception(this + " cannot add " + aDataset + ", " + aDataset.getSize() + " required, " 
					+ (freeSpace()) + " available");
		}
	}
	
	public double utilisation()
	{
		double lUtilisation = 0;
		for(int i = 0; i < fDatasets.size(); i++)
		{
			lUtilisation += fDatasets.get(i).getSize();
		}
		
		return lUtilisation / fSize;
	}
	
	public double freeSpace()
	{
		double lUtilisation = 0;
		for(int i = 0; i < fDatasets.size(); i++)
		{
			lUtilisation += fDatasets.get(i).getSize();
		}
		
		double availableSize;
		if(isAtMaxCapacity)
			availableSize = fSize * fP_max;
		else
			availableSize = fSize * fP_ini;
		
		return availableSize - lUtilisation;
	}
	
	public void setDatasets(ArrayList<DataSet> aDatasets)
	{
		fDatasets = aDatasets;
	}
	public String getName()
	{
		return fName;
	}
	public void setName(String aName)
	{
		fName = aName;
	}
	public double getP_ini()
	{
		return fP_ini;
	}
	public void setP_ini(double aP_ini)
	{
		fP_ini = aP_ini;
	}
	public double getP_max()
	{
		return fP_max;
	}
	public void setP_max(double aP_max)
	{
		fP_max = aP_max;
	}
	public double getSize()
	{
		return fSize;
	}
	public void setSize(double aSize)
	{
		fSize = aSize;
	}
	
	public String toString()
	{
		return fName;
	}

    /**
     * @return the hasFixedData
     */
    public boolean hasFixedData() {
        return hasFixedData;
    }

    /**
     * @param hasFixedData the hasFixedData to set
     */
    public void setHasFixedData(boolean hasFixedData) {
        this.hasFixedData = hasFixedData;
    }
	
}
