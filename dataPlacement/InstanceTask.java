package dataPlacement;

import java.util.ArrayList;

public class InstanceTask extends Task
{
	protected ArrayList<Task> fNewInstanceTasks;
	protected String fInstanceName;
	
	public InstanceTask(ArrayList<Task> aInstanceTasks)
	{
		fNewInstanceTasks = aInstanceTasks;
	}
	
	public void setInstanceName(String aInstanceName)
	{
		fInstanceName = aInstanceName;
	}
	
	public String getInstanceName()
	{
		return fInstanceName;
	}
	
	public ArrayList<Task> getInstanceTasks()
	{
		return fNewInstanceTasks;
	}
}
