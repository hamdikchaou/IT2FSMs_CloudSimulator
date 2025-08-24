package dataPlacement;

import java.util.ArrayList;

import realworkflow.Log;

public class DataSet
{
	private String fName;
	private double fSize;
	private ArrayList<Task> fTasks;
	private DataCenter fDC;
	private DataCenter fixedAddress;

	private boolean fExists;
	private boolean fWasGenerated;

	public DataSet()
	{
		fTasks = new ArrayList<Task>();
		fWasGenerated = false;
	}
	
	public void setExists(boolean aExists)
	{
		fExists = aExists;
	}
	
	public boolean exists()
	{
		return fExists;
	}
	
	public DataCenter getDC()
	{
		return fDC;
	}

	public void setDC(DataCenter aDC)
	{
		fDC = aDC;
	}

	public String getName()
	{
		return fName;
	}

	public void setName(String aName)
	{
		fName = aName;
	}

	public double getSize()
	{
		return fSize;
	}

	public void setSize(double aSize)
	{
		fSize = aSize;
	}

	public ArrayList<Task> getTasks()
	{
		return fTasks;
	}
	
	public void addTask(Task aTask)
	{
		if(!fTasks.contains(aTask))
			fTasks.add(aTask);
	}

	public void setTasks(ArrayList<Task> aTasks)
	{
		fTasks = aTasks;
	}
	
	public String toString()
	{
		return fName;
	}
	
	public void afficheDataset(DataSet fdata)
	{
		Log.print("Dataset:"  +fdata.getName());
        Log.print(" Size:"  + fdata.getSize());
        Log.print(" Generated:"  + fdata.wasGenerated());
        Log.printLine();
	}
    
	
	public void setWasGenerated(boolean aWasGenerated)
	{
		fWasGenerated = aWasGenerated;
	}
	
	public boolean wasGenerated()
	{
		return fWasGenerated;
	}

    /**
     * @return the fixedAddress
     */
    public DataCenter getFixedAddress() {
        return fixedAddress;
    }

    /**
     * @param fixedAddress the fixedAddress to set
     */
    public void setFixedAddress(DataCenter fixedAddress) {
        this.fixedAddress = fixedAddress;
    }
}
