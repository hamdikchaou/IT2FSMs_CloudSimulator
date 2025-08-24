package dataPlacement;

import java.util.ArrayList;
import java.util.List;

import realworkflow.Log;

public class Task
{
	protected String fName;
	protected ArrayList<DataSet> fInput;
	protected ArrayList<DataSet> fOutput;
	
	protected ArrayList<Task> fParents;
	protected ArrayList<Task> fChildren;
	
	/*
     * The list of all files (input data and ouput data)
     */
//  private List<FileItem> fileList;
    
	
	public Task()
	{
		fInput = new ArrayList<DataSet>();
		fOutput = new ArrayList<DataSet>();
		fParents = new ArrayList<Task>();
		fChildren = new ArrayList<Task>();
	}
	
	public ArrayList<Task> getChildren()
	{
		return fChildren;
	}
	
	public ArrayList<Task> getParents()
	{
		return fParents;
	}
	
	public ArrayList<DataSet> getInput()
	{
		return fInput;
	}
	public void setInput(ArrayList<DataSet> aInput)
	{
		fInput = aInput;
	}
	public String getName()
	{
		return fName;
	}
	public void setName(String aName)
	{
		fName = aName;
	}
	public ArrayList<DataSet> getOutput()
	{
		return fOutput;
	}
	public void setOutput(ArrayList<DataSet> aOutput)
	{
		fOutput = aOutput;
	}
	
	public void addInput(DataSet aDataset)
	{
		for(int i = 0; i < fInput.size(); i++)
			if(fInput.get(i).getName().compareTo(aDataset.getName()) == 0)
				return;
		
		fInput.add(aDataset);
	}
	
	public void removeInput(DataSet aDataset)
	{
		for(int i = 0; i < fInput.size(); i++)
			if((fInput.get(i).getName().equals(aDataset.getName())) && (fInput.get(i).getSize() == aDataset.getSize()) )
				fInput.remove(fInput.get(i));
	}
	
	public void addOutput(DataSet aDataset)
	{
		for(int i = 0; i < fOutput.size(); i++)
			if(fOutput.get(i).getName().compareTo(aDataset.getName()) == 0)
				return;
		
		fOutput.add(aDataset);
	}
	
	public void removeOutput(DataSet aDataset)
	{
		for(int i = 0; i < fOutput.size(); i++)
			if((fOutput.get(i).getName().equals(aDataset.getName())) && (fOutput.get(i).getSize() == aDataset.getSize()) )
				fOutput.remove(aDataset);
	}
	
	public boolean isReady()
	{
		for(int i = 0; i < fInput.size(); i++)
		if(!fInput.get(i).exists())
			return false;
		
		return true;
	}
	
	public void addParent(Task aTask)
	{
		if(!fParents.contains(aTask))
			fParents.add(aTask);
		
		aTask.addAsChild(this);
	}
	
	protected void addAsParent(Task aTask)
	{
		if(!fParents.contains(aTask))
			fParents.add(aTask);
	}
	
	public void addChild(Task aTask)
	{
		if(!fChildren.contains(aTask))
			fChildren.add(aTask);
		
		aTask.addAsParent(this);
	}
	
	protected void addAsChild(Task aTask)
	{
		if(!fChildren.contains(aTask))
			fChildren.add(aTask);
	}
	
	public int getChildCount()
	{
		return fChildren.size();
	}
	
	public boolean canParent(Task aChild)
	{
		//am I the root?
		if(fParents.size() == 0)
			return true;
		//no, is this potential child my parent?
		else if(fParents.contains(aChild))
			return false;
		//is it the parent of any of my parents?
		else 
		{
			for(int i = 0; i < fParents.size(); i++)
				if(!fParents.get(i).canParent(aChild))
					return false;
		}
		
		return true;
	}
	
	public String toString()
	{
		return fName;
	}
}
