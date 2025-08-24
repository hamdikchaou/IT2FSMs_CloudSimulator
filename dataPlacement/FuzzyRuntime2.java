package dataPlacement;

import java.util.ArrayList;
import java.util.HashMap;

import workflow.Workflow;


public class FuzzyRuntime2 extends RuntimeAlgorithm
{
	public FuzzyRuntime2(BuildTimeAlgorithm aBuilder)
	{
		super(aBuilder);
	}

	/* (non-Javadoc)
	 * @see dataPlacement.RuntimeAlgorithm#setDatacenters(java.util.ArrayList)
	 */
	public void setDatacenters(ArrayList<DataCenter> aDatacenters)
	{
		fDatacenters = aDatacenters;
	}
	/* (non-Javadoc)
	 * @see dataPlacement.RuntimeAlgorithm#getTasks()
	 */
	public ArrayList<Task> getTasks()
	{
		return fTasks;
	}
	/* (non-Javadoc)
	 * @see dataPlacement.RuntimeAlgorithm#setTasks(java.util.ArrayList)
	 */
	public void setTasks(ArrayList<Task> aTasks)
	{
		fTasks = aTasks;
	}
	
	
	
	/* (non-Javadoc)
	 * @see dataPlacement.RuntimeAlgorithm#run()
	 */
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
		fTotalDataSentSize = 0;
		fTotalDataRetrievedSize = 0;
		fTotalDataRescheduled = 0;
        fTotalDataReschedules = 0;
		fTotalDataRescheduledSize = 0; 
		
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
			ArrayList<Task> lReadyTasks = getReadyTasks();
			
			for(int i = 0; i < lReadyTasks.size(); i++)
			{
				//execute the task on the center with the most of it's dependancies...
				DataCenter bestCenter = null;
				int bestCenterMissingSetCount = Integer.MAX_VALUE;
				
				ArrayList<DataSet> lMissingSets = new ArrayList<DataSet>();
				
				for(int j = 0; j < fUsedDatacenters.size(); j++)
				{
					ArrayList<DataSet> lTheseMissingSets = findMissingDataSets(lReadyTasks.get(i), fUsedDatacenters.get(j));
					//if this center is missing less datasets than the previous best
					if(lTheseMissingSets.size() < bestCenterMissingSetCount)
					{
						//this one becomes the best
						bestCenter = fUsedDatacenters.get(j);
						lMissingSets = lTheseMissingSets;
						bestCenterMissingSetCount = lMissingSets.size();
						//if it isn't missing any sets, you can't do any better
						// so stop looking
						if(lMissingSets.size() == 0)
							break;
					}
					//is this missing the same amount of datasets as the current best?
					else if(lTheseMissingSets.size() == bestCenterMissingSetCount)
					{
						//sum the clustering with the best center
						int bestCenterClustering = 0;
						int thisCenterClustering = 0;
						for(int k = 0; k < lReadyTasks.get(i).getOutput().size(); k++)
						{
							bestCenterClustering += calculateClustering(lReadyTasks.get(i).getOutput().get(k), bestCenter);
							thisCenterClustering += calculateClustering(lReadyTasks.get(i).getOutput().get(k), fUsedDatacenters.get(j));
						}
						
						if(thisCenterClustering > bestCenterClustering)
						{
							//this one becomes the best
							bestCenter = fUsedDatacenters.get(j);
							lMissingSets = lTheseMissingSets;
							bestCenterMissingSetCount = lMissingSets.size();
						}
						else if(thisCenterClustering == bestCenterClustering)
						{
							//the best is the least used one
							if(bestCenter.utilisation() > fUsedDatacenters.get(j).utilisation())
							{
								//this one becomes the best
								bestCenter = fUsedDatacenters.get(j);
								lMissingSets = lTheseMissingSets;
								bestCenterMissingSetCount = lMissingSets.size();
							}
						}
					}
				}
				
				lScheduledTasks += lReadyTasks.get(i) + ":" + bestCenter + "; ";
				
				try
				{
					//for each missing set, "stream" it from it's current center
					for(int j = 0; j < lMissingSets.size(); j++)
					{
						//where is it now?
						DataCenter lSource = lMissingSets.get(j).getDC();
						
						//System.out.println("Retrieving dataset " + lMissingSets.get(j).getName() + " from " +
						//					lSource.getName() + " to " + bestCenter.getName());
						
						lDatasetsRetrieved += lReadyTasks.get(i) + ":" + lMissingSets.get(j) + ":" + lSource + "->"+ bestCenter + "; ";
						lRetrievedCount++;
						lRetrievedSize += lMissingSets.get(j).getSize();
					}
				
					//now that all the retrievals have been recorded, we can execute the task
					// this sets the outputs of the task to "existing"
					ArrayList<DataSet> lNewData = bestCenter.execute(lReadyTasks.get(i));
					//remove the task from the todo list
					fTasks.remove(lReadyTasks.get(i));
				
					//for each of the outputs, we need to send them to the best place
					for(int j = 0; j < lNewData.size(); j++)
					{
						//where is the best place?
						DataCenter lTargetCenter = distributeNewDataFuzzy(lNewData.get(j), bestCenter);
						
						//can the desired target fit the new data?
						if(lTargetCenter.freeSpace() < lNewData.get(j).getSize())
						{
							//if it can't, we must redistribute all the data!
							lScheduleChanges = bestCenter + " executing " + lReadyTasks.get(i) + 
												" and sending the result (" + lNewData.get(j).getSize() + ") to " + lTargetCenter + " required a reschedule\r\n";
							lScheduleChanges += adjustSchedule(lNewData);
							scheduleChanged = true;
							
							//fTasks.remove(lReadyTasks.get(i));
							
							//all the data has been redistributed, now we need to start a new round
							break;
						}
						
						//store the data at the target center
						lTargetCenter.addDataset(lNewData.get(j));
						lNewData.get(j).setDC(lTargetCenter);
						
						//if it needed to be sent (ie, it wasn't stored locally)
						if(lTargetCenter != bestCenter)
						{
							//add to the count of data that was "Sent"
							lDatasetsSent += lNewData.get(j) + ":" + bestCenter + "->" + lTargetCenter + "; ";
							lSentCount++;
							lSentSize += lNewData.get(j).getSize();
						}
					}
					
					//did executing this task trigger the start of a new instance?
					if(lReadyTasks.get(i) instanceof InstanceTask)
					{
						//This task executing has triggered a new instance to start!
						lScheduleChanges = bestCenter + " executing " + lReadyTasks.get(i)
									+ " triggered a new instance.\r\n";
						
						//initiate the instance and redistribute the data
						lScheduleChanges += startNewInstance((InstanceTask)(lReadyTasks.get(i)));
						
						scheduleChanged = true;
						
						//fTasks.remove(lReadyTasks.get(i));
						
						break;
					}
					
					//if the schedule has changed, force a new round to start 
					//by clearing the ready task list
					if(scheduleChanged)
						lReadyTasks.clear();
						
				}
				catch(Exception ex)
				{
					//The datacenter couldn't execute the task for some reason.
					System.out.println("ERROR : " + ex.getMessage());
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
			
			//if the scheduleChanged during the round, we want to add the details to 
			// the report
			if(scheduleChanged)
				fReport += lScheduleChanges;
			
			fReport += "\r\n";
		}	
		//compile the totals for this run through the workflow
		fReport += "Total Retrieved:     " + fTotalDataRetrieved + " (" + fTotalDataRetrievedSize + ")\r\n";
		fReport += "Total Sent:          " + fTotalDataSent + " (" + fTotalDataSentSize + ")\r\n";
		fReport += "Total Redistributed: " + fTotalDataRescheduled + " (" + fTotalDataRescheduledSize + ")\r\n";
		fReport += "                     -------------\r\n";
		fReport += "Total Movement:      " + (fTotalDataRetrieved + fTotalDataSent + fTotalDataRescheduled) + " (" + (fTotalDataRetrievedSize + fTotalDataSentSize + fTotalDataRescheduledSize) + ")\r\n";
	}
	
	private String startNewInstance(InstanceTask aInstanceTask) throws Exception
	{
		String result = "";
		result += cleanupFootprint();
		
		result += "State before rescheduling:\r\n";
		
		Matrix lNewMatrix = new Matrix();
		result += getDataCenterState(lNewMatrix);
		
		//for each of the tasks in the new instance, link the data that each requires
		// and add the task to the task list
		for(int i = 0; i < aInstanceTask.getInstanceTasks().size(); i++)
		{
			Task thisTask = aInstanceTask.getInstanceTasks().get(i);
			for(int j = 0; j < thisTask.getInput().size(); j++)
			{
				thisTask.getInput().get(j).addTask(thisTask);
				//if the dataset exists, add it to the matrix also
				if(thisTask.getInput().get(j).exists())
					lNewMatrix.addDataset(thisTask.getInput().get(j));
			}
			
			fTasks.add(thisTask);
		}
		
		return result + redistribute(lNewMatrix);
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
		
		//work out the new distribution
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
		//this method returns a report of the states of each datacenter that is used
		//but also populates the Matrix that is supplied.
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
		//returns just a string representation of the used datacenters
		// without requiring the caller to supply a Matrix
		return getDataCenterState(new Matrix());
	}
	
	private DataCenter distributeNewData(DataSet aNewDataset, DataCenter aDatasetSource)
	{
		//find the datacenter that is the most relevant (the data on that center
		// is most clustered with the new set) by dynamicaly calculating the dependancy
		
		//if there are many centers with the same relevancy (same dependancy score)
		// and one of them is the dataset source (where the data was generated), we
		// want to leave it there (ie, store it locally). Otherwise, we want to move
		// it to the datacenter that is the least loaded.
		
		int bestClustering = Integer.MIN_VALUE;
		DataCenter bestCenter = null;
		
		for(int i = 0; i < fUsedDatacenters.size(); i++)
		{
			int thisClustering = calculateClustering(aNewDataset, fUsedDatacenters.get(i));
			if(thisClustering > bestClustering)
			{
				bestCenter = fUsedDatacenters.get(i);
				bestClustering = thisClustering;
			}
			else if (thisClustering == bestClustering)
			{
				if(fUsedDatacenters.get(i) == aDatasetSource
					|| bestCenter == aDatasetSource)
				{
					//we want to leave it where it was generated
					bestCenter = aDatasetSource;
				}
				else if(bestCenter.utilisation() > fUsedDatacenters.get(i).utilisation())
				{
					//put it on the center that is least loaded
					bestCenter = fUsedDatacenters.get(i);
				}
			}
		}
		
		return bestCenter;
	}
	
	private DataCenter distributeNewDataFuzzy(DataSet aNewDataset, DataCenter aDataCenterSource)
	{
		FCM fcm = new FCM(aNewDataset, fUsedDatacenters);
		fcm.init();
		fcm.membership_matrix();
	//	fcm.cendroid_converge();
	//	int [] clust =fcm.cluster();

		DataCenter bestCenter = fcm.cluster(aDataCenterSource);

		return bestCenter;
	}
	
}
