package test;

import java.util.ArrayList;
import java.util.LinkedList;
import dataPlacement.BEA;
import dataPlacement.BuildTimeAlgorithm;
import dataPlacement.Clusterer;
import dataPlacement.DataCenter;
import dataPlacement.DataSet;
import dataPlacement.IT2FcmPartition;
import dataPlacement.RuntimeAlgorithm;
import dataPlacement.Matrix;
import workflow.Workflow;

public class IT2FCMPSOSimulator extends CloudSimulator
{
	private RuntimeAlgorithm fRuntime;
	private BuildTimeAlgorithm fBuildtime;
	private Matrix fClusteredMatrix;
	private Clusterer fClusterer = new BEA();
	public String name = "Flexi";
	
	private LinkedList<Integer> fDataRetrieved, fDataSent, fDataRescheduled, fDataReschedules;
	private LinkedList<Double> fDataRetrievedSize, fDataSentSize, fDataRescheduledSize;
	private LinkedList<Double> fMovementAverage, fMovementStandardDeviation;
	private LinkedList<Double> fTaskExecutionAverage, fTaskExecutionStandardDeviation;
	
	public IT2FCMPSOSimulator(ArrayList<DataCenter> aDataCenters, RuntimeAlgorithm aRuntime, 
															  BuildTimeAlgorithm aBuildtime)
	{
		super(aDataCenters);
		fRuntime = aRuntime;
		fBuildtime = aBuildtime;
		fDataRetrieved = new LinkedList<Integer>();
		fDataSent = new LinkedList<Integer>();
		fDataRescheduled = new LinkedList<Integer>();
		fDataRetrievedSize = new LinkedList<Double>();
		fDataSentSize = new LinkedList<Double>();
		fDataRescheduledSize = new LinkedList<Double>();
		fDataReschedules = new LinkedList<Integer>();
		fMovementAverage = new LinkedList<Double>();
		fMovementStandardDeviation = new LinkedList<Double>();
		fTaskExecutionAverage = new LinkedList<Double>();
		fTaskExecutionStandardDeviation = new LinkedList<Double>();
	}

	public String simulateWorkflow(Workflow aWorkflow) throws Exception
	{
		String result = "";
		
		//run the buildtime algorithm on the workflow//calling buildtime
		ArrayList<DataCenter> lUtilisedCenters = fBuildtime.distribute(aWorkflow);
        System.out.println("Build time distribution completed_from_FlexiSimulator Class");
        System.out.println("--Utilised DataCenters ");
        for(DataCenter d : lUtilisedCenters){
            System.out.print(d.getName() + "= {" );
            for(DataSet ds : d.getDatasets()){
                System.out.print(ds.getName() + " | ");
            }
            System.out.print("}");
            System.out.println();
        }
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
			
			//reset the counters in the datacenters
			thisCenter.resetDataCenterCounts();
		}
		
		result += "\r\n";
		///////////////////////////////////////////////
		Matrix fDependancyMatrix = new Matrix(aWorkflow);
		fClusteredMatrix = fClusterer.cluster(fDependancyMatrix);
		/******************************************************************************************************/
		IT2FcmPartition ff = new IT2FcmPartition(lUtilisedCenters,fClusteredMatrix);
		lUtilisedCenters = ff.getFuzzyDC();
		System.out.println("fuzzy flexi-----------------------------"+lUtilisedCenters.toString());
		increaseDatacenterCapacity();
		System.out.println("-->Setting used DataCenters at maximum capacity = "+lUtilisedCenters.get(0).getSize());
		System.out.println("\n-->Going From BuildTime to the runtime stage (executing the workflow tasks by order)");
		fRuntime.run(lUtilisedCenters, aWorkflow);
		result += fRuntime.getReport();
		
        fDataReschedules.add(fRuntime.getTotalDataReschedules());
		fDataRescheduled.add(fRuntime.getTotalDataRescheduled());
		fDataRescheduledSize.add(fRuntime.getTotalDataRescheduledSize());
		fDataRetrieved.add(fRuntime.getTotalDataRetrieved());
		fDataRetrievedSize.add(fRuntime.getTotalDataRetrievedSize());
		fDataSent.add(fRuntime.getTotalDataSent());
		fDataSentSize.add(fRuntime.getTotalDataSentSize());
		
		calculateAverageAndSD();
		return result;
	}//////////////////////////end/////simulateWorkflow////////////////

	private void calculateAverageAndSD()
	{
		//calculate the average data movement to each center
		double lAverageDataMovement = 0, lAverageTaskExecution = 0;
		double lDataMovementSD = 0, lTaskExecutionSD = 0;
		
		for(int i = 0; i < fDatacenters.size(); i++)
		{
			lAverageDataMovement += fDatacenters.get(i).getDatasetMovementCount();
			lDataMovementSD += Math.pow(fDatacenters.get(i).getDatasetMovementCount(), 2);
			lAverageTaskExecution += fDatacenters.get(i).getTaskExecutionCount();
			lTaskExecutionSD += Math.pow(fDatacenters.get(i).getTaskExecutionCount(), 2);
		}
		
		lAverageDataMovement /= fDatacenters.size();
		lAverageTaskExecution /= fDatacenters.size();
		lDataMovementSD /= fDatacenters.size();
		lTaskExecutionSD /= fDatacenters.size();
		lDataMovementSD -= Math.pow(lAverageDataMovement, 2);
		lTaskExecutionSD -= Math.pow(lAverageTaskExecution, 2);
		lDataMovementSD = Math.sqrt(lDataMovementSD);
		lTaskExecutionSD = Math.sqrt(lTaskExecutionSD);
		
		fMovementAverage.add(lAverageDataMovement);
		fTaskExecutionAverage.add(lAverageTaskExecution);
		fMovementStandardDeviation.add(lDataMovementSD);
		fTaskExecutionStandardDeviation.add(lTaskExecutionSD);
	}

	@Override
	public String toString()
	{
		return name;
	}
	
	public double getAverageDataRescheduled()
	{
		return average(fDataRescheduled);
	}

    public double getAverageDataReschedules()
	{
		return average(fDataReschedules);
	}
	
	public double getAverageDataRetrieved()
	{
		return average(fDataRetrieved);
	}
	
	public double getAverageDataSent()
	{
		return average(fDataSent);
	}
	
	public double getAverageDataRescheduledSize()
	{
		return average(fDataRescheduledSize);
	}
	
	public double getAverageDataRetrievedSize()
	{
		return average(fDataRetrievedSize);
	}
	
	public double getAverageDataSentSize()
	{
		return average(fDataSentSize);
	}
	
	public double getMovementAverage()
	{
		return average(fMovementAverage);
	}
	
	public double getMovementSD()
	{
		return average(fMovementStandardDeviation);
	}
	
	public double getTaskExecutionAverage()
	{
		return average(fTaskExecutionAverage);
	}
	
	public double getTaskExecutionSD()
	{
		return average(fTaskExecutionStandardDeviation);
	}
}