package dataPlacement;

import java.util.ArrayList;
import java.util.HashMap;

import workflow.Workflow;

public class DumbRuntime extends RuntimeAlgorithm
{
	public DumbRuntime(BuildTimeAlgorithm aBuilder)
	{
		super(aBuilder);
	}
	
	public void run(ArrayList<DataCenter> aDatacenters, Workflow aWorkflow) throws DistributionException
	{
		fTasks = aWorkflow.getTasks();
		fDatacenters = aDatacenters;
		fUsedDatacenters = new ArrayList<DataCenter>();
		for(int i = 0; i < fDatacenters.size(); i++)
		{
			if(fDatacenters.get(i).getDatasets().size() > 0)
				fUsedDatacenters.add(fDatacenters.get(i));
		}
		fReport = "";
		
		int lRoundNumber = 1;
		fTotalDataSent = 0;
		fTotalDataRetrieved = 0;
		fTotalDataReschedules = 0;
        fTotalDataRescheduled = 0;
		fTotalDataSentSize = 0.0;
		fTotalDataRetrievedSize = 0.0;
		fTotalDataRescheduledSize = 0.0;
		
		
		//while there are tasks to execute...
		while(fTasks.size() > 0)
		{
			//start of the round
			System.out.println("New Round!");
			fReport += "Round " + lRoundNumber++ + "\r\n";
			String lScheduledTasks = "Scheduled Tasks:    ";
			String lDatasetsRetrieved = "Datasets Retrieved: ";
			String lDatasetsSent = "Datasets Sent:      ";
			boolean scheduleChanged = false;
			String lScheduleChanges = "";
			int lRetrievedCount = 0, lSentCount = 0;
			double lRetrievedSize = 0.0, lSentSize = 0.0; 
			
			fReport += cleanupFootprint();
			
			//go over the list of tasks and see which ones are ready to execute
			ArrayList<Task> lReadyTasks = new ArrayList<Task>();
			for(int i = 0; i < fTasks.size(); i++)
			{
				if(fTasks.get(i).isReady())
				{
					lReadyTasks.add(fTasks.get(i));
				}
			}
			
			for(int i = 0; i < lReadyTasks.size(); i++)
			{
				//execute the task on the center with the most of it's dependancies...
				DataCenter bestCenter = null;
				int bestCenterCount = Integer.MAX_VALUE;
				
				ArrayList<DataSet> lMissingSets = new ArrayList<DataSet>();
				
				for(int j = 0; j < fUsedDatacenters.size(); j++)
				{
					ArrayList<DataSet> lTheseMissingSets = findMissingDataSets(lReadyTasks.get(i), fUsedDatacenters.get(j));
					if(lTheseMissingSets.size() < bestCenterCount)
					{
						bestCenter = fUsedDatacenters.get(j);
						lMissingSets = lTheseMissingSets;
						bestCenterCount = lMissingSets.size();
						if(lMissingSets.size() == 0)
							break;
					}
				}
				
				lScheduledTasks += lReadyTasks.get(i) + ":" + bestCenter + "; ";
				
				try
				{
					//for each missing set, copy/move it from it's current center
					for(int j = 0; j < lMissingSets.size(); j++)
					{
						//where is it now?
						DataCenter lSource = lMissingSets.get(j).getDC();
						//move it to bestCenter
						//bestCenter.addDataset(lMissingSets.get(j));
						System.out.println("Retrieving dataset " + lMissingSets.get(j).getName() + " from " +
											lSource.getName() + " to " + bestCenter.getName());
						
						lDatasetsRetrieved += lReadyTasks.get(i) + ":" + lMissingSets.get(j) + ":" + bestCenter + "; ";
						lRetrievedCount++;
						lRetrievedSize += lMissingSets.get(j).getSize();
					}
				
					ArrayList<DataSet> lNewData = bestCenter.execute(lReadyTasks.get(i));
				
					for(int j = 0; j < lNewData.size(); j++)
					{
						//when new data is generated, we just store it locally
						if(bestCenter.freeSpace() < lNewData.get(j).getSize()){
							lScheduleChanges += adjustSchedule(lNewData);
                            scheduleChanged = true;
                        }
						
						bestCenter.addDataset(lNewData.get(j));
						lNewData.get(j).setDC(bestCenter);
					}
					if(!scheduleChanged)
						fTasks.remove(lReadyTasks.get(i));
				}
				catch(Exception ex)
				{
					//The datacenter couldn't execute the task for some reason.
					System.out.println("ERROR : " + ex);
					System.out.println(ex.getMessage());
                    if(ex instanceof DistributionException)
                        throw new DistributionException(ex.getMessage());
					return;
				}
				
			}
			
			//compile the report
			fReport += lScheduledTasks + "\r\n";
			fReport += lDatasetsRetrieved + "Total: " + lRetrievedCount + " (" + lRetrievedSize + ")\r\n";
			fTotalDataRetrieved += lRetrievedCount;
			fTotalDataRetrievedSize += lRetrievedSize;
			fReport += lDatasetsSent + "Total: " + lSentCount + " (" + lSentSize + ")\r\n";
			fTotalDataSent += lSentCount;
			fTotalDataSentSize += lSentSize;
			
			if(scheduleChanged){
				fReport += lScheduleChanges;
                scheduleChanged = false;
            }
			
			fReport += "\r\n";
		}	
		fReport += "Total Retrieved: " + fTotalDataRetrieved + " (" + fTotalDataRetrievedSize + ")\r\n";
		fReport += "Total Sent:      " + fTotalDataSent + " (" + fTotalDataSentSize + ")\r\n";
		fReport += "                 -------------\r\n";
		fReport += "Total Movement:  " + (fTotalDataRetrieved + fTotalDataSent) + " (" + (fTotalDataRetrievedSize + fTotalDataSentSize) + ")\r\n";
		
		return;
	}
	
	private String adjustSchedule(ArrayList<DataSet> aNewDatasets) throws Exception
	{
        fTotalDataReschedules++;
		String result = "";
		result += cleanupFootprint();
		
		result += "State before rescheduling:\r\n";
		
		Matrix lNewMatrix = new Matrix();
		
		for(int i = 0; i  < aNewDatasets.size(); i++)
			lNewMatrix.addDataset(aNewDatasets.get(i));
		
		result += getDataCenterState(lNewMatrix);
		
		return result + redistribute(lNewMatrix);
	}

	private String redistribute(Matrix lNewMatrix) throws Exception
	{
		//store the old allocation so that we know what moved after the redistribution
		int totalMoves = 0;
		double totalMoved = 0.0;
		String lMoveSummary = "";
		HashMap<String, ArrayList<DataSet>> lOldAllocation = new HashMap<String, ArrayList<DataSet>>();
		
		for(int i = 0; i < fDatacenters.size(); i++)
		{
			lOldAllocation.put(fDatacenters.get(i).getName(), new ArrayList<DataSet>(fDatacenters.get(i).getDatasets()));
		}
		
		for(DataCenter d : fBuilder.fDataCenters)
            d.setMaxCapacity(false);
        for(DataCenter d : fDatacenters)
            d.setMaxCapacity(false);
		fUsedDatacenters = fBuilder.distribute(lNewMatrix.getDatasets());
		
		//now calculate what needs to move
		for(int i = 0; i < fUsedDatacenters.size(); i++)
		{
			ArrayList<DataSet> lOldList, lNewList;
			lOldList = lOldAllocation.get(fUsedDatacenters.get(i).getName());
			lNewList = fUsedDatacenters.get(i).getDatasets();
			
			//go through the newlist and see if each item is missing from the old list
			for(int j = 0; j < lNewList.size(); j++)
			{
				if(!lOldList.contains(lNewList.get(j)))
				{
					//it is missing! therefore it was moved to this datacenter
					//during the redistribution
					totalMoves++;
					totalMoved += lNewList.get(j).getSize();
					lMoveSummary += " - Dataset " + lNewList.get(j) + " moved to " + fUsedDatacenters.get(i) + "\r\n";
				}	
			}
		}
		
		String result = "State after rescheduling:\r\n";
		
		result += getDataCenterState();
		
		result += lMoveSummary;
		result += "Total movement during this redistribution:\r\n\t" + totalMoves + " (" + totalMoved + ")\r\n";
		
		fTotalDataRescheduled += totalMoves;
		fTotalDataRescheduledSize += totalMoved;
		for(DataCenter d : fBuilder.fDataCenters)
            d.setMaxCapacity(true);
        for(DataCenter d : fDatacenters)
            d.setMaxCapacity(true);
		return result;
	}

	private String getDataCenterState(Matrix lNewMatrix)
	{
		String result = "";
		
		for(int i = 0; i < fUsedDatacenters.size(); i++)
		{
			DataCenter thisCenter = fUsedDatacenters.get(i);
			result += thisCenter.getName() + " : ";
			for(int j = 0; j < thisCenter.getDatasets().size(); j++)
			{
				result += thisCenter.getDatasets().get(j).getName() + " (" 
						+ thisCenter.getDatasets().get(j).getSize() + "); ";
				
				lNewMatrix.addDataset(thisCenter.getDatasets().get(j));
			}
			
			result += "\r\n";
		}
		
		return result;
	}
	
	private String getDataCenterState()
	{
		return getDataCenterState(new Matrix());
	}
}
