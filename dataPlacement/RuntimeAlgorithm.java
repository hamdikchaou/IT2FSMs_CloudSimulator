package dataPlacement;

import java.util.ArrayList;

import workflow.Workflow;

public abstract class RuntimeAlgorithm
{
	protected ArrayList<Task> fTasks;
	protected ArrayList<DataCenter> fDatacenters;
	protected ArrayList<DataCenter> fUsedDatacenters;
	
	protected int fTotalDataRetrieved, fTotalDataSent, fTotalDataRescheduled, fTotalDataReschedules;
	protected double fTotalDataRetrievedSize, fTotalDataSentSize, fTotalDataRescheduledSize;
	protected BuildTimeAlgorithm fBuilder;
	protected String fReport;
	
	public RuntimeAlgorithm(BuildTimeAlgorithm aBuildTimeAlgorithm)
	{
		fBuilder = aBuildTimeAlgorithm;
	}

	public String getReport()
	{
		return fReport;
	}

	public abstract void run(ArrayList<DataCenter> aDataCenters, Workflow aWorkflow) throws DistributionException;
	
	protected String cleanupFootprint()
	{
		String result = "";
		//foreach datacenter
		for(int i = 0; i < fUsedDatacenters.size(); i++)
		{
			//for each set of data in that center
			for(int j = 0; j < fUsedDatacenters.get(i).getDatasets().size(); j++)
			{
				DataSet thisDataset = fUsedDatacenters.get(i).getDatasets().get(j);
				
				//if this data was generated (we can't safely delete existing data
				// because it may be used by an unknown task in a future instance)
				if(thisDataset.wasGenerated())
				{
					boolean canDelete = true;
					//see if any upcoming tasks require it
					for(int k = 0; k < fTasks.size(); k++)
					{
						if(fTasks.get(k).getInput().contains(thisDataset))
							canDelete = false;
					}
					
					//if it's safe to delete this data, do it
					if(canDelete)
					{
						fUsedDatacenters.get(i).getDatasets().remove(thisDataset);
						result += thisDataset + " (" + thisDataset.getSize() + ") " 
								+ " is no longer needed and was deleted from " + fUsedDatacenters.get(i) + "\r\n";
						
						//removing this dataset will cause all the datasets after this one 
						// in the datacenter's list of datasets to be shifted left
						// so we need to move the index back too.
						j--;
					}
				}
			}
		}
		
		return result;
	}
	
	protected ArrayList<Task> getReadyTasks()
	{
		ArrayList<Task> result = new ArrayList<Task>();
		for(int i = 0; i < fTasks.size(); i++)
		{
			if(fTasks.get(i).isReady()) //contient des inputs datasets
			{
				result.add(fTasks.get(i));
                                //System.out.println("the order***********???????"+fTasks.get(i).getName());
			}
		}
		return result;	//retourne un ready task
	}
	
	protected ArrayList<DataSet> findMissingDataSets(Task aTask, DataCenter aDataCenter)
	{
		//of all the datasets required to execute aTask, which ones are NOT
		// present at aDatacenter?
		ArrayList<DataSet> result = new ArrayList<DataSet>();
		for(int i = 0; i < aTask.getInput().size(); i++)
		{
			if(!aDataCenter.getDatasets().contains(aTask.getInput().get(i)))
				result.add(aTask.getInput().get(i));
		}
		
		return result;
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

	public int getTotalDataRetrieved()
	{
		return fTotalDataRetrieved;
	}
	
	public double getTotalDataRetrievedSize()
	{
		return fTotalDataRetrievedSize;
	}
	
	public int getTotalDataSent()
	{
		return fTotalDataSent;
	}
	
	public double getTotalDataSentSize()
	{
		return fTotalDataSentSize;
	}
	
	public int getTotalDataRescheduled()
	{
		return fTotalDataRescheduled;
	}

    public int getTotalDataReschedules()
	{
		return fTotalDataReschedules;
	}
	public double getTotalDataRescheduledSize()
	{
		return fTotalDataRescheduledSize;
	}
}