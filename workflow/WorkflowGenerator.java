package workflow;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;

import dataPlacement.*;

public class WorkflowGenerator
{
	public double maxDataSize = 128;
	public double minDataSize = 1;
	public double maxGeneratedDataSize = 128;
	public double minGeneratedDataSize = 1;
	
	public int dataCount;
	public int taskCount;
	public int maxDataUsage;
	public int maxTaskBranch;
	private Random fRandom;
	
	private ArrayList<Task> fTasks;
	private ArrayList<DataSet> fDataSets;
	private ArrayList<DataSet> fGeneratedDataSets;
	
	public WorkflowGenerator(	int aDatasetCount, 
								int aTaskCount, 
								int aDataUsageFactor, 
								int aTaskBranchFactor)
	{
		dataCount = aDatasetCount;
		taskCount = aTaskCount;
		maxDataUsage = aDataUsageFactor;
		maxTaskBranch = aTaskBranchFactor;
		
		fRandom = new Random();
	}
	
	public WorkflowGenerator()
	{
		this(5,5,4,3);
	}
	
	public Workflow generate()
	{
		fTasks = new ArrayList<Task>();
		fDataSets = new ArrayList<DataSet>();
		fGeneratedDataSets = new ArrayList<DataSet>();
		
		Workflow result = new Workflow();
		//first generate the tasks
		System.out.print("Generating tasks... ");
		result.setTasks(generateTasks());
		System.out.println("Done");
		System.out.print("Generating Datasets... ");
		result.setDatasets(generateDatasets());
		System.out.println("Done");
		System.out.print("Linking tasks... ");
		linkTasks();
		System.out.println("Done");
		result.setGeneratedDatasets(fGeneratedDataSets);
		
		return result;
	}
	
	private ArrayList<Task> generateTasks()
	{
		for(int i = 0; i < taskCount; i++)
		{
			Task thisTask = new Task();
			thisTask.setName("t" + (i + 1));
			fTasks.add(thisTask);
		}
		
		return fTasks;
	}
	
	private ArrayList<DataSet> generateDatasets()
	{
		for(int i = 0; i < dataCount; i++)
		{
			//generate a new dataset
			DataSet thisDataset = new DataSet();
			fDataSets.add(thisDataset);
			thisDataset.setName("d" + (i+1));
			thisDataset.setSize(fRandom.nextDouble() * (maxDataSize - minDataSize) + minDataSize);
		//	thisDataset.setSize(1000);
			thisDataset.setExists(true);
			
			int thisDatasetUsage = fRandom.nextInt(maxDataUsage) + 1;
			
			while(thisDataset.getTasks().size() < thisDatasetUsage)
			{
				//pick a random task
				Task lTask = fTasks.get(fRandom.nextInt(fTasks.size()));
				//is this dataset already used in this task?
				if(!thisDataset.getTasks().contains(lTask))
				{
					//no. add it
					lTask.getInput().add(thisDataset);
					thisDataset.addTask(lTask);
				}
			}
		}
		
		return fDataSets;
	}
	
	private void linkTasks()
	{
		LinkedList<Task> lTaskQueue;
		lTaskQueue = new LinkedList<Task>();
		ArrayList<Task> lGeneratedTasks = new ArrayList<Task>();
		
		lTaskQueue.add(fTasks.get(0));
		
		while(lTaskQueue.size() > 0)
		{
			Task lTask = lTaskQueue.pop();
			
			//is it possible for this task to have any children at all? 
			//(only false if the graph is supermaximally connected)
			ArrayList<Task> lPotentialChildren = getPotentialChildren(lTask);
			if(lPotentialChildren.size() == 0)
				continue;
			
			int lThisTasksChildCount = fRandom.nextInt(maxTaskBranch) + 1;
			
			
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
				if(!lGeneratedTasks.contains(lThisChild))
				{
					lTaskQueue.add(lThisChild);
					lGeneratedTasks.add(lThisChild);
				}
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
		lDataset.setSize(fRandom.nextDouble() * 
							(maxGeneratedDataSize - minGeneratedDataSize) 
							+ minGeneratedDataSize);
		lDataset.setExists(false);
		lDataset.setWasGenerated(true);
		
		fGeneratedDataSets.add(lDataset);
		
		return lDataset;
	}
	
	private ArrayList<Task> getPotentialChildren(Task aTask)
	{
		ArrayList<Task> result = new ArrayList<Task>();
		
		for(int i = 0; i < fTasks.size(); i++)
			if(aTask.canParent(fTasks.get(i)))
				result.add(fTasks.get(i));
		
		if(result.contains(aTask))
			result.remove(aTask);
		
		return result;
	}
}
