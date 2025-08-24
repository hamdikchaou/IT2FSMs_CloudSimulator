package test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Random;

import realworkflow.Log;

import workflow.*;
import dataPlacement.*;
import dataPlacement.Runtime;
import java.util.HashMap;
import java.lang.Math; 
import java.lang.Thread;
import java.lang.Runnable;
import workflow.WorkflowParser;


public class Test
{
	private ArrayList<DataCenter> fDatacenters;
//	private static WorkflowVisualisation visuel;
	
//	public static final int INITIAL_DATACENTER_SIZE = 2500;//espace libre au debut
//	public static final int RUNTIME_DATACENTER_SIZE = 100000;
	
	public static int dataCenterCount = 4;
	public static double dataCenterSize = 240.0;//taille d'un datacenter 
	public static double dataCenterPercent = 0.0;
	public static double dataCenterSizeShift = 0.0;
	public static double dataCenterPIni = 0.30;
	public static double dataCenterPMax = 1.0;
	
	public static int dataSetCount;
	public static int dataSetCountInit;
	public static int dataSetCountGen;
	public static double dataSetMaxSize = 1024;
	public static double dataSetMinSize = 1;
	public static int dataSetMaxUse = 3;
	
	public static int taskCount = 80;
	public static double taskResultMaxSize = 1024;
	public static double taskResultMinSize = 1;
	public static int taskMaxBranch = 4;
        public static double FIXED_DATASETS = 0.0;
	
	private int fTestCount;
	public static int fFailedTestCount=0;
	
	public static double datasetsSize;
	
	private ArrayList<CloudSimulator> fSimulators;
	
	private WorkflowGenerator fGenerator;
	
	public Test()
	{
	//	visuel = new WorkflowVisualisation();
	//	visuel.lunch();
		fDatacenters = new ArrayList<DataCenter>();
		configureDataCenters();
		fSimulators = generateSimulators();//strategies
		configureGenerator();//affichage simulateur
		fTestCount =0;
	}
	public static WorkflowParser wp ;
	public static String daxPath;
	
	public static void main(String[] args) throws Exception 
	{
		Chronometer ch = new Chronometer();
		ch.start();
		
	//	MultiWorkflowGenerator lGenerator = new MultiWorkflowGenerator();
	//	Workflow lWork = lGenerator.generate();
		
		daxPath = "/C:/dax/Montage_25.xml";
		
		//Montage_25_50_100_1000
		//CyberShake_30_50_100_1000
		//Epigenomics_24_46_100_997
		//Inspiral_30_50_100_1000
		//Sipht_30_60_100_1000
		
        File daxFile = new File(daxPath);
        if (!daxFile.exists()) {
            Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
            return;
        }
        wp= new WorkflowParser(daxPath);
        Workflow lWork = wp.parse();
        lWork.save("workflow.original");
        dataSetCount = lWork.getDatasets().size() + lWork.getGeneratedDatasets().size();
        dataSetCountInit = lWork.getDatasets().size();
        dataSetCountGen = lWork.getGeneratedDatasets().size();
        Log.printLine("Initial datasets number="+lWork.getDatasets().size());
        Log.printLine("Generated datasets number="+lWork.getGeneratedDatasets().size());
        
        datasetsSize = computeDatasetsSize(lWork); 
        Log.printLine("datasetsSize="+datasetsSize);
        
        Test t = new Test();
   /*     for (int f=0;f<1;f++){
    		System.out.println("*-*-**-*-**-*-**-*-**-*-**-*-****** WORKFLOW NÂ°"+f+" ******-*-**-*-**-*-**-*-**-*-**-*-*");
                MultiWorkflowGenerator lGenerator = new MultiWorkflowGenerator();
		Workflow lWork = lGenerator.generate();*/
        ArrayList <DataSet>datasets = lWork.getDatasets();
        datasets = lWork.renameinitdatasets(datasets);
        lWork.setDatasets(datasets);
       
        //lWork = lWork.renameinitdatasets(lWork);
        
        ArrayList <DataSet> gdatasets = lWork.getGeneratedDatasets();
        gdatasets = lWork.renamegendatasets(gdatasets);
        lWork.setGeneratedDatasets(gdatasets);
        
        //lWork.afficheDatasets(lWork.getGeneratedDatasets());
        
        //lWork.afficheTasks(lWork.getTasks());
        //System.out.println("Tasks Number = "+lWork.getTasks().size());
        
        ArrayList <Task> tasks = lWork.getTasks();
        tasks = lWork.renametasks(tasks);
        lWork.setTasks(tasks);
       
		//save the workflow to a file
		try
		{
			lWork.save("workflow.work");
		}
		catch(IOException ex)
		{
			System.out.println("ERROR: " + ex.getMessage());
			fFailedTestCount++;
		}
		
//		setupDataCenters(); //erreur
		try{
			t.doTest("Hamdi.Final.Workflow", lWork);
		}catch(Exception e){
			System.out.println("error_here");
		}
		
		t.saveFinalReport("workflow.final");
		
		ArrayList<CloudSimulator> fSimulators = new ArrayList<CloudSimulator>();
		//fSimulators.add(new BuildTimeOnlySimulator(fDatacenters));
		//fSimulators.add(new RunTimeOnlySimulator(fDatacenters));
		//fSimulators.add(new DumbSimulator(fDatacenters));
		//fSimulators.add(new SmartSimulator(t.fDatacenters));
		
	//	System.out.println("ERROR: " + fSimulators.size());

		for(int i = 0; i < fSimulators.size(); i++)
		{
			//reset the workflow
			lWork.reset();

			//run the workflow
			String report = fSimulators.get(i).simulateWorkflow(new Workflow(lWork));
			
			//save the report
			try
			{
				PrintStream out = new PrintStream(args[0] + "-" + fSimulators.get(i).toString() + ".out");
				out.println(report);
				out.flush();
				out.close();
			}
			catch(Exception ex)
			{
				System.out.println("ERROR: " + ex.getMessage());
			}
		}
        //}//test random
		t.saveFinalReport("workflow.final");
		ch.stop();
		System.out.println("\n"+"Execution time: [" + ch.getTime()+"] milliseconds ");
	}// end main
		
	static Double computeDatasetsSize (Workflow w)
	{
		Double d = 0.0;
		for (int i=0;i<w.getDatasets().size();i++)
			d+=w.getDatasets().get(i).getSize();
		for (int i=0;i<w.getGeneratedDatasets().size();i++)
			d+=w.getGeneratedDatasets().get(i).getSize();
		
		return d;
	}
	
	//creation et affectation de taille aux datacenters
	public void configureDataCenters()
	{
		Random lRandom = new Random();
		dataCenterSize = (int) (datasetsSize-1.5*(datasetsSize/dataCenterCount)) ;
		dataCenterSize = (int) (datasetsSize/dataCenterCount) ;
		dataCenterPercent = 0.5;
		dataCenterSize = (double) (datasetsSize)*dataCenterPercent;
		
		for(int i = 0; i < dataCenterCount; i++)
		{
			DataCenter thisDC = new DataCenter();
			thisDC.setName("dc" + (i + 1));
			//calculate a size for this DC
			double thisShift = dataCenterSizeShift * lRandom.nextDouble();
			
			if(lRandom.nextBoolean())
				thisShift *= -1;
			double thisCenterSize = dataCenterSize + (thisShift * dataCenterSize);
			//double thisCenterSize = dataCenterSize;
			thisDC.setSize(dataCenterSize);
			thisDC.setP_ini(dataCenterPIni);
			thisDC.setP_max(dataCenterPMax);
			
			fDatacenters.add(thisDC);
		}
	}
        
       /* public void configureDataCenters()
	{
		Random lRandom = new Random();
//		ArrayList<DataCenter>fDatacenters = new ArrayList<DataCenter>();
		
		for(int i = 0; i < dataCenterCount; i++)
		{
			DataCenter thisDC = new DataCenter();
			thisDC.setName("dc" + (i + 1));
			//calculate a size for this DC
			double thisShift = dataCenterSizeShift * lRandom.nextDouble();
			
			if(lRandom.nextBoolean())
				thisShift *= -1;
			double thisCenterSize = dataCenterSize + (thisShift * dataCenterSize);
			//thisCenterSize = 800;
			thisDC.setSize(thisCenterSize);
			thisDC.setP_ini(dataCenterPIni);
			thisDC.setP_max(dataCenterPMax);
			
			fDatacenters.add(thisDC);
		//	System.out.println("get== "+dataCenterSizeShift);
		}
	//	System.out.println("configure"+fDatacenters.toString());
	}
	*/
	
	private void configureGenerator()
	{
		fGenerator = new WorkflowGenerator();
		fGenerator.dataCount = dataSetCount;
		fGenerator.maxDataSize = dataSetMaxSize;
		fGenerator.minDataSize = dataSetMinSize;
		fGenerator.maxDataUsage = dataSetMaxUse;
		fGenerator.taskCount = taskCount;
		fGenerator.maxGeneratedDataSize = taskResultMaxSize;
		fGenerator.minGeneratedDataSize = taskResultMinSize;
		fGenerator.maxTaskBranch = taskMaxBranch;
	}
	
	public void doTest(String aTestPrefix, Workflow lWork) throws Exception
	{
		resetDataCenters();//les datacenters encore vide
        System.out.println("--------------Organising fixed data for every DataCenter---Randomly-----------");
        Workflow lTreeWork = new Workflow(lWork);
        ArrayList<DataSet> lDataSets = lWork.getDatasets();
        Random lRandom = new Random();
        
        HashMap<DataCenter, Double> space = new HashMap<DataCenter,Double>();
        for(DataCenter d : fDatacenters)
            space.put(d, d.freeSpace());//reserver un espace libre ds chaque datacenter
        System.out.println("DataCenter_free_space:"+space);
        int q = 0;
        //choisir de fixer des datasets
        for(int i = 0; i < lDataSets.size() * FIXED_DATASETS; i++)
        {
            DataSet aDataset = lDataSets.get(i);
            DataCenter aDatacentre = fDatacenters.get(java.lang.Math.abs(lRandom.nextInt() % fDatacenters.size()));
            System.out.println("DataSet["+aDataset+"]="+aDataset.getSize()+" will be fixed at DataCenter:"+aDatacentre);
            if(space.get(aDatacentre) >= aDataset.getSize()){
                space.put(aDatacentre, space.get(aDatacentre) - aDataset.getSize());
                aDataset.setFixedAddress(aDatacentre);
                aDataset.setName(aDataset.getName() + "_f_" + aDataset.getFixedAddress());
            }
            else{
                i--;
                q++;
                if(q > 100){
                    i++;
                    q = 0;
                }
            }
        }
        System.out.println("DataCenter_free_space_after fixing datasets:"+space);
        //System.out.println("datasets"+lDataSets);
        System.out.println("--------------Fixed data has been arranged--------------");
   
        lTreeWork.setDatasets(lDataSets);

		for(int i = 0; i < fSimulators.size(); i++)
		{
			System.out.println("****************************************  " + fSimulators.get(i).toString()+"  ****************************************");
			System.out.print("-->calling_class_FlexiSimulator");
			//reset the workflow
		//	lWork.reset();//reset the generated datasets
		//	lTreeWork.reset();
			//reset the datacenters
		//	resetDataCenters();
			
			//run the workflow with this simulator
            String report=null;
            
     ///       if(!fSimulators.get(i).toString().contains("Tree"))
            
            	//lancement de la simulation vers FlexiSimulator
          //      report = fSimulators.get(i).simulateWorkflow(new Workflow(lTreeWork));
            
      //      else
      //      System.out.println("vide");
            // report = fSimulators.get(i).simulateWorkflow(new Workflow(lWork));
			
			//save the report
			try
			{	
				report = fSimulators.get(i).simulateWorkflow(new Workflow(lTreeWork));
				//System.out.println("here "+report);
				PrintStream out = new PrintStream(aTestPrefix + "-" + fSimulators.get(i).toString() + ".out");
				out.println(report);
				out.flush();
				out.close();
			}
			catch(Exception ex)
			{
				System.out.println("ERROR : " + ex.getMessage());
			}
		}
		fTestCount++;
	}

	private ArrayList<CloudSimulator> generateSimulators()
	{
		ArrayList<CloudSimulator> result = new ArrayList<CloudSimulator>();
		

/*		
		BuildTime lBuilder2 = new BuildTime(fDatacenters);
		FlexiSimulator sim2 = new FlexiSimulator(fDatacenters, new DumbRuntime(lBuilder2), lBuilder2);
		sim2.name = "BuildOnly";
		result.add(sim2);
	
/*        TreeBuildTimeNearestSize lBuilder3 = new TreeBuildTimeNearestSize(fDatacenters);
		FlexiSimulator sim3 = new FlexiSimulator(fDatacenters, new DumbRuntime(lBuilder3), lBuilder3);
		sim3.name = "TreeBuildOnly";
		result.add(sim3);*/

/*		DumbBuildTime lBuilder4 = new DumbBuildTime(fDatacenters);
		FlexiSimulator sim4 = new FlexiSimulator(fDatacenters, new Runtime(lBuilder4), lBuilder4);
		sim4.name = "RunOnly";
		result.add(sim4);*/

/*		DumbBuildTime lBuilder1 = new DumbBuildTime(fDatacenters);
		FlexiSimulator sim1 = new FlexiSimulator(fDatacenters, new DumbRuntime(lBuilder1), lBuilder1);
		sim1.name = "Random";
		result.add(sim1);
*/		
/*		BuildTime lBuilder5 = new BuildTime(fDatacenters);
		FlexiSimulator sim5 = new FlexiSimulator(fDatacenters, new Runtime(lBuilder5), lBuilder5);
		sim5.name = "Yuan";
		result.add(sim5);
		
/*		BuildTime lBuilder8 = new BuildTime(fDatacenters);
		FlexiFuzzySimulator sim8 = new FlexiFuzzySimulator(fDatacenters, new FuzzyRuntime(lBuilder8), lBuilder8);
		sim8.name = "DFCM";
		result.add(sim8);
*/
/*		BuildTime lBuilder12 = new BuildTime(fDatacenters);
		IT2FCMSimulator sim12 = new IT2FCMSimulator(fDatacenters, new FuzzyRuntime(lBuilder12), lBuilder12);
		sim12.name = "IT2DFCM";
		result.add(sim12);
*/                
                BuildTime lBuilder15 = new BuildTime(fDatacenters);
		IT2FSMSimulator sim15 = new IT2FSMSimulator(fDatacenters, new FuzzyRuntime(lBuilder15), lBuilder15);
		sim15.name = "T-2FSMs ";
		result.add(sim15);
                
                   
/*                BuildTime lBuilder16 = new BuildTime(fDatacenters);
		IT2GAsim sim16 = new IT2GAsim(fDatacenters, new FuzzyRuntimeGA(lBuilder16), lBuilder16);
		sim16.name = "IT2-GA ";
		result.add(sim16);
 
/*             BuildTime lBuilder13 = new BuildTime(fDatacenters);
		IT2FCMSimulator sim13 = new IT2FCMSimulator(fDatacenters, new FuzzyRuntimePSO(lBuilder13), lBuilder13);
		sim13.name = "IT2FCMPSO";
		result.add(sim13);
 /*  /*    
  /*          BuildTime lBuilder14 = new BuildTime(fDatacenters);
		FlexiSimulator sim14 = new FlexiSimulator(fDatacenters, new FuzzyRuntimePSO(lBuilder14), lBuilder14);
		sim14.name = "PSO";
		result.add(sim14);

	
//my simulators
/*		BuildTime lBuilder6 = new BuildTime(fDatacenters);
		FlexiFuzzySimulator sim6 = new FlexiFuzzySimulator(fDatacenters, new Runtime(lBuilder6), lBuilder6);
		sim6.name = "FuzzyBuildandRun";
		result.add(sim6);
		
		BuildTime lBuilder10 = new BuildTime(fDatacenters);
		IT2FCMSimulator sim10 = new IT2FCMSimulator(fDatacenters, new Runtime(lBuilder10), lBuilder10);
		sim10.name = "SIT2FCM";
		result.add(sim10);
*/		

		
/*		BuildTime lBuilder11 = new BuildTime(fDatacenters);
		FCMPSOSimulator sim11 = new FCMPSOSimulator(fDatacenters, new Runtime(lBuilder11), lBuilder11);
		sim11.name = "FCMPSO_BuildandRun";
		result.add(sim11);
//*/		
/*		BuildTime lBuilder7 = new BuildTime(fDatacenters);
		FlexiSimulator sim7 = new FlexiSimulator(fDatacenters, new FuzzyRuntime(lBuilder7), lBuilder7);
		sim7.name = "BuildandFuzzyRun";
		result.add(sim7);


		
/*	BuildTime lBuilder9 = new BuildTime(fDatacenters);
		FlexiSimulator sim9 = new FlexiSimulator(fDatacenters, new FuzzyRuntime2(lBuilder9), lBuilder9);
		sim9.name = "BuildandFuzzyRun2";
		result.add(sim9);		

/*        TreeBuildTimeNearestSize lBuilder6 = new TreeBuildTimeNearestSize(fDatacenters);
		FlexiSimulator sim6 = new FlexiSimulator(fDatacenters, new Runtime(lBuilder6), lBuilder6);
		sim6.name = "TreeBuildandRun";
		result.add(sim6); */
		/*
		DumbBuildTime lBuilder5 = new DumbBuildTime(fDatacenters);
		FlexiSimulator sim5 = new FlexiSimulator(fDatacenters, new NewRuntime(lBuilder5), lBuilder5);
		sim5.name = "NewRuntimeOnly";
		result.add(sim5);
		
		BuildTime lBuilder6 = new BuildTime(fDatacenters);
		FlexiSimulator sim6 = new FlexiSimulator(fDatacenters, new NewRuntime(lBuilder6), lBuilder6);
		sim6.name = "BuildAndNewRun";
		result.add(sim6);
		*/
		
		return result;
	}
	
	private void resetDataCenters()
	{
		//reset all the datacenters
		for(int i = 0; i < fDatacenters.size(); i++)
		{
			fDatacenters.get(i).resetDataCenter();
		}
	}
	
	public void saveFinalReport(String reportName)
	{
		PrintWriter out;
		
		try
		{
			out = new PrintWriter(reportName);
		}
		catch(Exception ex)
		{
			System.out.println("Cannot save Final Report");
			System.out.println(ex);
			System.out.println(ex.getMessage());
			return;
		}
		
		out.println("Final Report");
		out.println("============");
		out.println("Tests: " + fTestCount);
		out.println("Tests Failed: " + fFailedTestCount);
		out.println("Parameters:");
		out.println("  Data Centers: " + dataCenterCount);
		out.println("  |  p_ini: " + dataCenterPIni);
		out.println("  |  p_max: " + dataCenterPMax);
		out.println("  |  size(one center):  " + dataCenterSize + "=|datasetsSize|*|"+dataCenterPercent+"|");
		out.println("  |  shift: " + (dataCenterSizeShift * 100) + "%");
		out.println("  -----------");
		out.println();
		out.println("  Workflows:");
		out.println("  |  DataSets Count: " + dataSetCount+"  |  DataSets Size: " + datasetsSize);
		out.println("  |  Initial DataSets Count: " + dataSetCountInit);
		out.println("  |  Generated DataSets Count: " + dataSetCountGen);
		//out.println("  |  DataSet Max Usage: " + dataSetMaxUse);
		out.println("  |  Task Count: " + daxPath);
		//out.println("  |  Max Task Branching: " + taskMaxBranch);
		//out.println("  |  Task Result Max Size: " + taskResultMaxSize);
		//out.println("  |  Task Result Min Size: " + taskResultMinSize);
                out.println("  |  Fixed DataSet Percentage: " + FIXED_DATASETS * 100 + "%");
		out.println("  --------------------------");
		out.println();
		
		int maxSimNameLength = 0;
		
		for(int i = 0; i < fSimulators.size(); i++)
		{
			if(fSimulators.get(i).toString().length() > maxSimNameLength)
				maxSimNameLength = fSimulators.get(i).toString().length(); 
		}
		
		for(int i = 0; i < fSimulators.size(); i++)
		{
			double totalMoved  = 0;
			double totalMovedSize = 0;
			out.print(padWithSpaces(fSimulators.get(i).toString(), maxSimNameLength) + ": ");
			out.println("Data Retrieved:   " + fSimulators.get(i).getAverageDataRetrieved() + " (" + fSimulators.get(i).getAverageDataRetrievedSize() + ")");
			totalMoved += fSimulators.get(i).getAverageDataRetrieved();
			totalMovedSize += fSimulators.get(i).getAverageDataRetrievedSize();
			out.print(padWithSpaces("", maxSimNameLength + 2));
			out.println("Data Sent:        " + fSimulators.get(i).getAverageDataSent() + " (" + fSimulators.get(i).getAverageDataSentSize() + ")");
			totalMoved += fSimulators.get(i).getAverageDataSent();
			
		//	out.println("Data Fuzzy Sent:  " + fSimulators.get(i).getAverageFuzzyDataSent() + " (" + fSimulators.get(i).getAverageFuzzyDataSentSize() + ")");
		//	totalMoved += fSimulators.get(i).getAverageDataSent();
			
			totalMovedSize += fSimulators.get(i).getAverageDataSentSize();
			out.print(padWithSpaces("", maxSimNameLength + 2));
			out.println("Data Rescheduled: " + fSimulators.get(i).getAverageDataRescheduled() + " (" + fSimulators.get(i).getAverageDataRescheduledSize() + ")");
                        out.print(padWithSpaces("", maxSimNameLength + 2));
                        out.println("Number of Reschedules: " + fSimulators.get(i).getAverageDataReschedules());
			totalMoved += fSimulators.get(i).getAverageDataRescheduled();
			totalMovedSize += fSimulators.get(i).getAverageDataRescheduledSize();
			out.print(padWithSpaces("", maxSimNameLength + 2));
			out.println("Total Movement:   " + totalMoved + " (" + totalMovedSize + ")");
			out.println();
			out.print(padWithSpaces("", maxSimNameLength + 2));
			out.println("Average Dataset Movement Std Dev: " + fSimulators.get(i).getMovementSD());
			out.print(padWithSpaces("", maxSimNameLength + 2));
			out.println("Average Task Execution Std Dev: " + fSimulators.get(i).getTaskExecutionSD());
			out.println();
		}
		
		out.flush();
		out.close();
		
	}
	
	public static String padWithSpaces(String aString, int length)
	{
		String result = aString;
		
		while(result.length() < length)
			result = " " + result;
		
		return result;
	}

}
