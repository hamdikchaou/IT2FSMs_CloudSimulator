package workflow;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;

import dataPlacement.*;
import test.Chronometer;

public class MultiWorkflowGenerator
{
	public ArrayList<DataCenter> fDatacenters;
	
	private final int MAX_DATA_SIZE = 128;
	private final int MIN_DATA_SIZE = 1;
	
	private int fDataCount;
	private int fTaskCount;
	private int fMaxDataUsageFactor;
	private int fMaxTaskBranchFactor;
	private int fMaxInstanceCount;
	private Random fRandom;
	
	private ArrayList<Task> fTasks;
	private LinkedList<ArrayList<Task>> fInstances;
	private ArrayList<DataSet> fDataSets;
	private ArrayList<DataSet> fGeneratedDataSets;
	
	public MultiWorkflowGenerator(	int aDatasetCount, 
								int aTaskCount, 
								int aDataUsageFactor, 
								int aTaskBranchFactor,
								int aMaxInstanceCount)
	{
		fDataCount = aDatasetCount;
		fTaskCount = aTaskCount;
		fMaxDataUsageFactor = aDataUsageFactor;
		fMaxTaskBranchFactor = aTaskBranchFactor;
		fMaxInstanceCount = aMaxInstanceCount;
		
		fRandom = new Random();
	}
	
	public MultiWorkflowGenerator()
	{
		this(5 ,5, 3, 2, 2);
	}
	
	public Workflow generate()
	{
		fTasks = new ArrayList<Task>();
		fDataSets = new ArrayList<DataSet>();
		fGeneratedDataSets = new ArrayList<DataSet>();
		
		Workflow result = new Workflow();
		System.out.println("--------------Creation of a Workflow--------------");
		
		//first generate the tasks
		System.out.print("Generating tasks... ");
		result.setTasks(generateTasks());
		System.out.println("Done");
		System.out.println("Number of tasks instances = "+fInstances.size());
		System.out.println(fInstances.toString());
		
		System.out.print("Generating initial Datasets... ");
		result.setDatasets(generateDatasets());
		System.out.println("Done");
		System.out.println(fDataSets.toString());
		
		System.out.print("Linking tasks... ");
		for(int i = 0; i < fInstances.size(); i++)
			linkTasks(fInstances.get(i));
		System.out.println("Done");
		
		System.out.print("New Generated Datasets... ");
		result.setGeneratedDatasets(fGeneratedDataSets);
		System.out.println("Done");
		System.out.println(fGeneratedDataSets.toString());
		
		return result;
	}
	
	private ArrayList<Task> generateTasks()
	{
		fInstances = new LinkedList<ArrayList<Task>>();
		int instanceCount =1;
		//= Math.max(fRandom.nextInt(fMaxInstanceCount) + 1, 2);
		
		//int taskNumber = 1;
		for(int j = instanceCount; j > 0; j--)
		{
			ArrayList<Task> thisInstance = new ArrayList<Task>();
			for(int k = 0; k < fTaskCount; k++)
			{
				int thisTaskNumber = j * fTaskCount - (fTaskCount - 1) + k;
				Task thisTask = new Task();
				thisTask.setName("t" + (thisTaskNumber++));
				fTasks.add(thisTask);
				thisInstance.add(thisTask);
			}
			//have other instances already been created?
			if(fInstances.size() > 0)
			{
				//if they have, we want to link one of the tasks in this instance to the next one
				//pick a random task
				//get all the indexes of the task to replace
				int lInstanceTaskIndex = fRandom.nextInt(thisInstance.size());
				Task lInstanceTask = thisInstance.get(lInstanceTaskIndex);
				int lTaskListIndex = fTasks.indexOf(lInstanceTask);
				
				//make a new InstanceTask to replace it using the instance currently in position 0
				//the instance in position 0 will be triggered by this new InstanceTask
				InstanceTask lNewTask = new InstanceTask(fInstances.get(0));
				
				//set the new task's name to be identical
				lNewTask.setName(lInstanceTask.getName());
				
				//replace the existing Task with the new InstanceTask
				fTasks.set(lTaskListIndex, lNewTask);
				thisInstance.set(lInstanceTaskIndex, lNewTask);
				//System.out.println("++++++heyyyyyy "+fInstances.size());
				
			}
			
			//add the new instance at the front (it is now the primary because nothing triggers it)
			fInstances.addFirst(thisInstance);
		}
		
		return fInstances.get(0);
	}
	
	private ArrayList<DataSet> generateDatasets()
	{
		for(int i = 0; i < fDataCount; i++)
		{
			//generate a new dataset
			DataSet thisDataset = new DataSet();
			fDataSets.add(thisDataset);
			thisDataset.setName("d" + (i+1));
		//	thisDataset.setSize(fRandom.nextDouble() * (MAX_DATA_SIZE - MIN_DATA_SIZE) + MIN_DATA_SIZE);
			thisDataset.setSize(fRandom.nextInt(MAX_DATA_SIZE) + MIN_DATA_SIZE);
			
		//	thisDataset.setSize(ti[i]);
			thisDataset.setExists(true);
			
			int thisDatasetUsage = fRandom.nextInt(fMaxDataUsageFactor) + 1;
			
			/////////////////////////////////////////////////////////////////////
			while(thisDataset.getTasks().size() < thisDatasetUsage)
			{
				//pick a random task
				Task lTask = fTasks.get(fRandom.nextInt(fTasks.size()));
				//Task lTask = fTasks.get(i);
				//is this dataset already used in this task?
				if(!thisDataset.getTasks().contains(lTask))
				{
					//no. add it
					lTask.getInput().add(thisDataset);
					
					/* if the task is in the Primary instance, add it to the
					 * dataset's list of tasks. 
					 * Otherwise, only add the dataset to the tasks list of datasets
					 * This is to prevent the clustering
					 * of the primary instance from including tasks that share datasets
					 * in instances that haven't been triggered yet.
					 */
					if(fInstances.get(0).contains(lTask))
						thisDataset.addTask(lTask);
					
				}
			}//////////////////////////////////////////////////////////////////////////
			
		}
		return fDataSets;
	}
	
	private void linkTasks()
	{
		LinkedList<Task> lTaskQueue;
		lTaskQueue = new LinkedList<Task>();
		
		lTaskQueue.add(fTasks.get(0));
		
		while(lTaskQueue.size() > 0)
		{
			Task lTask = lTaskQueue.pop();
			
			//is it possible for this task to have any children at all? 
			//(only false if the graph is supermaximally connected)
			ArrayList<Task> lPotentialChildren = getPotentialChildren(lTask);
			if(lPotentialChildren.size() == 0)
				continue;
			
			int lThisTasksChildCount = fRandom.nextInt(fMaxTaskBranchFactor) + 1;
			
			lThisTasksChildCount = Math.min(lThisTasksChildCount, lPotentialChildren.size());
			
			//get the generated dataset to be used as an input for the children of this task
			DataSet lDataset = getGeneratedDataset("du" + lTask.getName().substring(1));
			lTask.addOutput(lDataset);
			
			while(lTask.getChildCount() < lThisTasksChildCount)
			{
				//pick a task and make it this tasks child
				Task lThisChild = lPotentialChildren.get(fRandom.nextInt(lPotentialChildren.size()));
				if(lTask.canParent(lThisChild))
				{
					lThisChild.addParent(lTask);
					lPotentialChildren.remove(lThisChild);
				}
				
				lDataset.addTask(lThisChild);
				lThisChild.addInput(lDataset);
				lTaskQueue.add(lThisChild);
			}
		}
	}
	
	private void linkTasks(ArrayList<Task> aInstance)
	{
		LinkedList<Task> lTaskQueue;
		lTaskQueue = new LinkedList<Task>();
		
		lTaskQueue.add(aInstance.get(0));
		
		while(lTaskQueue.size() > 0)
		{
			Task lTask = lTaskQueue.pop();
			
			//is it possible for this task to have any children at all? 
			//(only false if the graph is supermaximally connected)
			ArrayList<Task> lPotentialChildren = getPotentialChildren(lTask);
			if(lPotentialChildren.size() == 0)
				continue;
			
			int lThisTasksChildCount = fRandom.nextInt(fMaxTaskBranchFactor) + 1;
			
			lThisTasksChildCount = Math.min(lThisTasksChildCount, lPotentialChildren.size());
			
			//get the generated dataset to be used as an input for the children of this task
			DataSet lDataset = getGeneratedDataset("du" + lTask.getName().substring(1));
			lTask.addOutput(lDataset);
			
			while(lTask.getChildCount() < lThisTasksChildCount)
			{
				//pick a task and make it this tasks child
				Task lThisChild = lPotentialChildren.get(fRandom.nextInt(lPotentialChildren.size()));
				if(lTask.canParent(lThisChild))
				{
					lThisChild.addParent(lTask);
					lPotentialChildren.remove(lThisChild);
				}
				
				lDataset.addTask(lThisChild);
				lThisChild.addInput(lDataset);
				lTaskQueue.add(lThisChild);
			}
		}
	}
	
	private DataSet getGeneratedDataset(String aSetName)
	{
		for(int i = 0; i < fGeneratedDataSets.size(); i++)
			if(fGeneratedDataSets.get(i).getName().compareTo(aSetName) == 0)
				return fGeneratedDataSets.get(i);
		
		DataSet lDataset = new DataSet();
		lDataset.setName(aSetName);
		lDataset.setSize(fRandom.nextInt(MAX_DATA_SIZE) + MIN_DATA_SIZE);
		lDataset.setExists(false);
		lDataset.setWasGenerated(true);
		
		fGeneratedDataSets.add(lDataset);
		
		return lDataset;
	}
	
	private ArrayList<Task> getPotentialChildren(Task aTask)
	{
		ArrayList<Task> result = new ArrayList<Task>();
		ArrayList<Task> lThisTasksInstance = getInstance(aTask);
		
		for(int i = 0; i < lThisTasksInstance.size(); i++)
			if(aTask.canParent(lThisTasksInstance.get(i)))
				result.add(lThisTasksInstance.get(i));
		
		if(result.contains(aTask))
			result.remove(aTask);
		
		return result;
	}
	
	private ArrayList<Task> getInstance(Task aTask)
	{
		for(int i = 0; i < fInstances.size(); i++)
			if(fInstances.get(i).contains(aTask))
				return fInstances.get(i);
		
		return null;
	}
}
