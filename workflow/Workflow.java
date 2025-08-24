package workflow;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;

import dataPlacement.DataSet;
import dataPlacement.Task;
import realworkflow.Log;

public class Workflow
{
	private ArrayList<DataSet> fDatasets;
	private ArrayList<Task> fTasks;
	private ArrayList<DataSet> fGeneratedDatasets;
	
	public Workflow()
	{
		fDatasets = new ArrayList<DataSet>();
		fGeneratedDatasets = new ArrayList<DataSet>();
		fTasks = new ArrayList<Task>();
	}
	
	public Workflow(Workflow aWorkflow)
	{
		fDatasets = new ArrayList<DataSet>(aWorkflow.getDatasets());
		fGeneratedDatasets = new ArrayList<DataSet>(aWorkflow.getGeneratedDatasets());
		fTasks = new ArrayList<Task>(aWorkflow.getTasks());
	}
	
	public ArrayList<DataSet> getDatasets()
	{
		return fDatasets;
	}
	public void setDatasets(ArrayList<DataSet> datasets)
	{
		fDatasets = datasets;
	}
	public ArrayList<DataSet> getGeneratedDatasets()
	{
		return fGeneratedDatasets;
	}
	public void setGeneratedDatasets(ArrayList<DataSet> generatedDatasets)
	{
		fGeneratedDatasets = generatedDatasets;
	}
	public ArrayList<Task> getTasks()
	{
		return fTasks;
	}
	public void setTasks(ArrayList<Task> tasks)
	{
		fTasks = tasks;
	}

	public void save(String fileName) throws IOException
	{
		
		PrintStream out = new PrintStream(fileName);
				
		out.println("datasets intilales");
		for(int i = 0; i < fDatasets.size(); i++)
		{
			out.println(fDatasets.get(i) + ":" + fDatasets.get(i).getSize());
		}
		out.println("");
		
		out.println("datasets générées");
		for(int i = 0; i < fGeneratedDatasets.size(); i++)
		{
			out.println(fGeneratedDatasets.get(i) + ":" + fGeneratedDatasets.get(i).getSize());
		}
		out.println("");
		
		out.println("tasks");	
		for(int i = 0; i < fTasks.size(); i++)
		{
			out.print(fTasks.get(i) + "-->");
			
			out.print("OutputDatasets=");
			for(int j = 0; j < fTasks.get(i).getOutput().size(); j++)
			{
				out.print(fTasks.get(i).getOutput().get(j) + ";");
				//+"("+fTasks.get(i).getOutput().get(j).getSize()+")"
			}
			
			//out.println(":");
			out.print(" |InputDatasets=");
			
			for(int j = 0; j < fTasks.get(i).getInput().size(); j++)
			{
				out.print(fTasks.get(i).getInput().get(j) + ";");
				//+"("+fTasks.get(i).getInput().get(j).getSize()+")"
			}
			out.println();
			out.print("predecessors tasks="+fTasks.get(i).getParents());
			out.println(" |successors tasks="+fTasks.get(i).getChildren());
			out.println();
		}
		
		
		out.println();
	}
	
	public void reset()
	{
		for(int i = 0; i < fGeneratedDatasets.size(); i++)
			fGeneratedDatasets.get(i).setExists(false);
	}
	
	/*public Workflow  renameinitdatasets(Workflow w)
	{
		ArrayList<DataSet> d = w.getDatasets();
		ArrayList<Task> t = w.getTasks();
		
		for(int i = 0; i < d.size(); i++)
		{
			for(int j = 0; j < t.size(); j++)
			{
				if (t.get(j).getInput().equals(d.get(i)))
						
			}
			d.get(i).setName("d" + (i+1));
			
		}
		
		return w;
	}*/
	
	public ArrayList<DataSet> renameinitdatasets(ArrayList<DataSet> data)
	{
		for(int i = 0; i < data.size(); i++)
		{
			data.get(i).setName("d" + (i+1));
		}	
		return data;
	}
	
	public ArrayList<DataSet> renamegendatasets(ArrayList<DataSet> data)
	{
		for(int i = 0; i < data.size(); i++)
		{
			data.get(i).setName("du" + (i+1));
		}	
		return data;
	}
	
	public ArrayList<Task> renametasks(ArrayList<Task> t)
	{
		for(int i = 0; i < t.size(); i++)
		{
			t.get(i).setName("t" + (i+1));
		}	
		return t;
	}
	
	public void afficheTasks(ArrayList<Task> fTasks)
	{
		for(int i=0;i<fTasks.size();i++)
        {
        	Log.print("Task:"  +fTasks.get(i).getName());
        	Log.print(" Childs:"  + fTasks.get(i).getChildCount());
        	Log.printLine(" Parents:"  + fTasks.get(i).getParents().size());
        	Log.printLine("Input datasets:"  +fTasks.get(i).getInput());
        	Log.printLine("Output datasets:"  +fTasks.get(i).getOutput());
        	Log.printLine();
        }
	}
	
	public void afficheDatasets(ArrayList<DataSet> ffdatasets)
	{
		for(int i=0;i<ffdatasets.size();i++)
        {
        	Log.print("DataSet:"  +ffdatasets.get(i).getName());
        	
        	Log.printLine("-->Used Tasks:"  +ffdatasets.get(i).getTasks());
        	Log.printLine();
        }
	}
	
	
	public void affiche(Workflow file) 
	{
		
		ArrayList<Task> fTasks = file.getTasks();
		
		System.out.println("tasks");	
		for(int i = 0; i < fTasks.size(); i++)
		{
			System.out.print(fTasks.get(i) + "-->");
			
			System.out.print("OutputDatasets=");
			for(int j = 0; j < fTasks.get(i).getOutput().size(); j++)
			{
				System.out.print(fTasks.get(i).getOutput().get(j) + ";");
			}
			
			//out.println(":");
			System.out.print(" |InputDatasets=");
			
			for(int j = 0; j < fTasks.get(i).getInput().size(); j++)
			{
				System.out.print(fTasks.get(i).getInput().get(j) + ";");
			}
			System.out.println();
			System.out.print("predecessors tasks="+fTasks.get(i).getParents());
			System.out.println(" |successors tasks="+fTasks.get(i).getChildren());
			System.out.println();
		}
		System.out.println();
	}
}
