package test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import workflow.Workflow;
import dataPlacement.DataCenter;
import dataPlacement.DistributionException;

public abstract class CloudSimulator
{
	protected ArrayList<DataCenter> fDatacenters;
	
	public abstract String simulateWorkflow(Workflow aWorkflow) throws DistributionException, Exception;
	
	public CloudSimulator(ArrayList<DataCenter> aDatacenters)
	{
		fDatacenters = aDatacenters;
	}
	
	public abstract String toString();
	
	protected void increaseDatacenterCapacity()
	{
		for(int i = 0; i < fDatacenters.size(); i++)
		{
			fDatacenters.get(i).setMaxCapacity(true);
		}
	}
	
	public double getAverageDataRescheduled()
	{
		return 0;
	}
	
	public double getAverageDataRetrieved()
	{
		return 0;
	}
	
	public double getAverageDataSent()
	{
		return 0;
	}
	
	public double getAverageDataRescheduledSize()
	{
		return 0;
	}
	
	public double getAverageDataRetrievedSize()
	{
		return 0;
	}
	
	public double getAverageDataSentSize()
	{
		return 0;
	}
	
	public double getMovementAverage()
	{
		return 0;
	}
	
	public double getMovementSD()
	{
		return 0;
	}
	
	public double getTaskExecutionAverage()
	{
		return 0;
	}
	
	public double getTaskExecutionSD()
	{
		return 0;
	}
    public double getAverageDataReschedules()
	{
		return 0;
	}
	
	protected double average(LinkedList<? extends Number> aList)
	{
		Iterator<? extends Number> iter = aList.iterator();
		double total = 0, count = 0;
		
		while(iter.hasNext())
		{
			count++;
			total += iter.next().doubleValue();
		}
		
		return total / count;
	}
	
	public double getAverageFuzzyDataSent()
	{
		return 0;
	}
	public double getAverageFuzzyDataSentSize()
	{
		return 0;
	}
}
