package test;

import java.util.ArrayList;

import dataPlacement.BuildTime;
import dataPlacement.DataCenter;
import dataPlacement.Runtime;
import dataPlacement.RuntimeAlgorithm;
import workflow.Workflow;

public class SmartSimulator extends CloudSimulator
{
	public SmartSimulator(ArrayList<DataCenter> aDatacenters)
	{
		super(aDatacenters);
	}
	
	@Override
	public String simulateWorkflow(Workflow aWorkflow)
	{
		String result = "";
		
		//use the buildtime to distribute data
		BuildTime lBuilder = new BuildTime(fDatacenters);
		
		try
		{
			ArrayList<DataCenter> lUtilisedCenters = lBuilder.distribute(aWorkflow);
		
			for(int i = 0; i < aWorkflow.getTasks().size(); i++)
			{
				result += aWorkflow.getTasks().get(i) + ": ";
				for(int j = 0; j < aWorkflow.getTasks().get(i).getInput().size(); j++)
				{
					result += aWorkflow.getTasks().get(i).getInput().get(j) + "; ";
				}
				result += "\r\n";
			}
			
			result += "\r\n";
			
			for(int i = 0; i < lUtilisedCenters.size(); i++)
			{
				DataCenter thisCenter = lUtilisedCenters.get(i);
				result += thisCenter.getName() + " : ";
				for(int j = 0; j < thisCenter.getDatasets().size(); j++)
				{
					result += thisCenter.getDatasets().get(j).getName() + " (" 
							+ thisCenter.getDatasets().get(j).getSize() + "); ";
				}
				
				result += "\r\n";
			}
			
			result += "\r\n";
			
			//increase the datacenter size
			increaseDatacenterCapacity();
			
			//use the real runtime to execute tasks
			RuntimeAlgorithm lRunner = new Runtime(lBuilder);
			
			lRunner.run(fDatacenters, aWorkflow);
			
			result += lRunner.getReport();
		}
		catch (Exception ex)
		{
			System.out.println("ERROR: " + ex.getMessage());
			System.exit(-1);
		}
		System.out.println("hounaaaaaa"+result);
		return result;
	}

	@Override
	public String toString()
	{
		return "BuildAndRun";
	}

}
