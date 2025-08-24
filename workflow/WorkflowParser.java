package workflow;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import realworkflow.Log;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import dataPlacement.*;

/**
 * WorkflowParser parse a DAX into tasks so that we can manage them
 * @date Feb 23, 2019
 * @author Hamdi Kchaou
 */
public class WorkflowParser {

	private ArrayList<DataSet> fDatasets;
	private ArrayList<Task> fTasks;
	private ArrayList<DataSet> fGeneratedDatasets;
	private Workflow wf;
	protected HashMap<String, DataSet> data;

	public String daxPath;
        
    public WorkflowParser(String daxPath) {
      
        this.daxPath = daxPath;     
    }
    
    public Workflow parse() 
    {
    	Workflow w = new Workflow();
    	if (this.daxPath != null) {    	
        	w = parseXmlFile(this.daxPath);
        }
        return w;
    }

    private Workflow parseXmlFile(String path) {
    	
    	fTasks = new ArrayList<Task>();
    	fDatasets = new ArrayList<DataSet>();
    	fGeneratedDatasets = new ArrayList<DataSet>();
 		DataSet thisDataset;
 		
 		data = new HashMap<>();
 		
    	try {
            SAXBuilder builder = new SAXBuilder();
            //parse using builder to get DOM representation of the XML file
            Document dom = builder.build(new File(path));
            Element root = dom.getRootElement();
            List<Element> list = root.getChildren();
            Task thisTask;
            for (Element node : list) {            	
            	switch (node.getName().toLowerCase()) {
                    case "job":
                        String nodeName = node.getAttributeValue("id");
                        thisTask = new Task();
        				thisTask.setName(nodeName);
        				
                        //getting datasets (input et output of every job (task))
        				List<Element> fileList = node.getChildren();
                        
        				for (Element file : fileList) {      	
        					if (file.getName().toLowerCase().equals("uses")) {
                            	
        						thisDataset = new DataSet();
        						String fileName = file.getAttributeValue("name");
                                if (fileName == null) {
                                    fileName = file.getAttributeValue("file");
                                }
                                if (fileName == null) {
                                    Log.print("Error in parsing xml");
                                }

                                String inout = file.getAttributeValue("link");
                                double size = 0.0;
                                
                                String fileSize = file.getAttributeValue("size");
                                if (fileSize != null) {
                                    size = Double.parseDouble(fileSize) /*/ 1024*/;
                                } else {
                                    Log.printLine("File Size not found for " + fileName);
                                }
                                
                                thisDataset.setName(fileName);
                                thisDataset.setSize(size);
                                thisDataset.setExists(true);
                             
                                if (inout.equals("output"))
                                {
                                	thisDataset.setWasGenerated(true);
                                	thisTask.addOutput(thisDataset);
                                }
                                else
                                {
                                	thisDataset.setWasGenerated(false);
                                	thisTask.addInput(thisDataset);
                                	//thisDataset.addTask(thisTask);
                                }
                                    
                               //if (!(fDatasets.contains(thisDataset)))
                                //fDatasets.add(thisDataset);
                               
                               String val = thisDataset.getName()+thisDataset.getSize()+thisDataset;
                                if (!(data.containsKey(val)))
                                {
                    
                                	data.put(val, thisDataset);
                                }
                                
                           /*     DataSet dataset = (DataSet) generated.get(valeur);
                                for(int i=0;i<fDatasets.size();i++)
                         	    {
                         			data.put(fDatasets.get(i).getName()+fDatasets.get(i).getSize()+fDatasets.get(i).wasGenerated(), fDatasets.get(i));
                         	    }*/
                         		
                            }//end if	   
        					
        				}//end for                        
        				fTasks.add(thisTask);
        				break;
        				
                    case "child":
                    	List<Element> pList = node.getChildren();
                        String childName = node.getAttributeValue("ref");
                        for (int i=0; i<fTasks.size();i++)
                        {
                        	//Task childTask = fTasks.get(fTasks.childName)
                        	String child = (String) fTasks.get(i).getName();
                        	if (child.equals(childName))
                        	{
                        		for (Element parent : pList) {
                        			String parentName = parent.getAttributeValue("ref");
                        			for (int j=0; j<fTasks.size();j++)
                                    {
                        				String parents = (String) fTasks.get(j).getName();
                                    	if (parents.equals(parentName))
                                    	{
                                    		Task parentTask = fTasks.get(j);
                                			fTasks.get(i).addParent(parentTask);
                                    	}
                                    }        
                        		}
                        	}
                        }
                        break;
                }//end switch
            }//end for
    
    	} catch (JDOMException jde) {
            Log.printLine("JDOM Exception;Please make sure your dax file is valid");

        } catch (IOException ioe) {
            Log.printLine("IO Exception;Please make sure dax.path is correctly set in your config file");

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Parsing Exception");
        }   

        //afficheTasks(fTasks); 
        // afficheDatasets(fGeneratedDatasets);
    	//Log.printLine(data.size());
    	fDatasets = extract(data);

    	fTasks = adjustTasks(path);
    	
        fGeneratedDatasets = generated(fDatasets);
        //fGeneratedDatasets = generated2();
        
        fDatasets = initialDatasets(fDatasets,fGeneratedDatasets);
        
        //adjustTasksDatasets(fTasks);
        
        //add workflow componements
    	wf = new Workflow();
    	wf.setTasks(fTasks);
    	wf.setDatasets(fDatasets);
    	wf.setGeneratedDatasets(fGeneratedDatasets);
    	return wf;
    }//end parsexml()
    
    private ArrayList<DataSet> extract(Map<String, DataSet> data)
    {
    	ArrayList<DataSet> d = new ArrayList<DataSet>();
    	for (Map.Entry mapentry : data.entrySet()) {
            //System.out.println("clé: "+mapentry.getKey() + " | valeur: " + mapentry.getValue());
            DataSet x = (DataSet) mapentry.getValue();
            d.add(x);
         }
    	return d;
    }
    
    private boolean cherche(ArrayList<DataSet> data, DataSet d) 
    {
    	boolean b=false;
    	for(int i=0;i<data.size();i++)
        {
    		if ((data.get(i).getName().equals(d.getName())) && (data.get(i).getSize()==d.getSize()))
    			b=true;
        }
    	return b;
    }
    
    private DataSet trouveDataset(ArrayList<DataSet> data, DataSet d) 
    {
    	DataSet b= new DataSet();
    	for(int i=0;i<data.size();i++)
        {
    		if ((data.get(i).getName().equals(d.getName())) && (data.get(i).getSize()==d.getSize()))
    			b=data.get(i);
        }
    	return b;
    }
       
    public ArrayList<Task> adjustTasks(String path)
	{
    	ArrayList<Task> fTasks = new ArrayList<Task>();
    	DataSet thisDataset;
 		
    	try {
    	SAXBuilder builder = new SAXBuilder();
        //parse using builder to get DOM representation of the XML file
        Document dom = builder.build(new File(path));
        Element root = dom.getRootElement();
        List<Element> list = root.getChildren();
        Task thisTask;
        for (Element node : list) {            	
        	switch (node.getName().toLowerCase()) {
                case "job":
                    String nodeName = node.getAttributeValue("id");
                    thisTask = new Task();
    				thisTask.setName(nodeName);
    				
                    //getting datasets (input et output of every job (task))
    				List<Element> fileList = node.getChildren();
                    
    				for (Element file : fileList) {      	
    					if (file.getName().toLowerCase().equals("uses")) {
                        	
    						thisDataset = new DataSet();
    						String fileName = file.getAttributeValue("name");
                            if (fileName == null) {
                                fileName = file.getAttributeValue("file");
                            }
                            if (fileName == null) {
                                Log.print("Error in parsing xml");
                            }

                            String inout = file.getAttributeValue("link");
                            double size = 0.0;
                            
                            String fileSize = file.getAttributeValue("size");
                            if (fileSize != null) {
                                size = Double.parseDouble(fileSize) /*/ 1024*/;
                            } else {
                                Log.printLine("File Size not found for " + fileName);
                            }
                            
                            thisDataset.setName(fileName);
                            thisDataset.setSize(size);
                            //thisDataset.setExists(true);
                            DataSet x = trouveDataset(fDatasets,thisDataset);
                            if (inout.equals("output"))
                            {
                            	thisTask.addOutput(x);
                            }
                            else
                            {
                            	thisTask.addInput(x);
                            	x.addTask(thisTask);
                            }     
                      }//end if	   
    					
    				}//end for                        
    				fTasks.add(thisTask);
    				break;
    				
                case "child":
                	List<Element> pList = node.getChildren();
                    String childName = node.getAttributeValue("ref");
                    for (int i=0; i<fTasks.size();i++)
                    {
                    	//Task childTask = fTasks.get(fTasks.childName)
                    	String child = (String) fTasks.get(i).getName();
                    	if (child.equals(childName))
                    	{
                    		for (Element parent : pList) {
                    			String parentName = parent.getAttributeValue("ref");
                    			for (int j=0; j<fTasks.size();j++)
                                {
                    				String parents = (String) fTasks.get(j).getName();
                                	if (parents.equals(parentName))
                                	{
                                		Task parentTask = fTasks.get(j);
                            			fTasks.get(i).addParent(parentTask);
                                	}
                                }        
                    		}
                    	}
                    }
                    break;
            }//end switch
        }//end for
        
    	} catch (JDOMException jde) {
            Log.printLine("JDOM Exception;Please make sure your dax file is valid");
    	} catch (IOException ioe) {
              Log.printLine("IO Exception;Please make sure dax.path is correctly set in your config file");
        } catch (Exception e) {
              e.printStackTrace();
              Log.printLine("Parsing Exception");
        }
    	
    	return fTasks;
	}
          
    //ajoute les liens des tasks dans les datasets
    public void adjustTasksDatasets(ArrayList<Task> fTasks)
    {
    	for(int i=0;i<fTasks.size();i++)
        {
    		for(int j=0;j<fTasks.get(i).getInput().size();j++)
            {
        		DataSet x = fTasks.get(i).getInput().get(j);
        		//Log.print("task="+fTasks.get(i));
        		//Log.printLine(" input="+x);
        		
        		for(int k=0;k<fDatasets.size();k++)
    				if((fDatasets.get(k).getName().equals(x.getName()))&&(fDatasets.get(k).getSize()==x.getSize())
    						&&(fDatasets.get(k).wasGenerated()==x.wasGenerated()))
    				{
    					fDatasets.get(k).addTask(fTasks.get(i));
    					//adjust tasks
    					fTasks.get(i).removeInput(x);
    					fTasks.get(i).addInput(fDatasets.get(k));
    				}
    			
    			for(int k=0;k<fGeneratedDatasets.size();k++)
    				if((fGeneratedDatasets.get(k).getName().equals(x.getName()))&&(fGeneratedDatasets.get(k).getSize()==x.getSize())
    						&&(fGeneratedDatasets.get(k).wasGenerated()==x.wasGenerated()))
    				{
    					fGeneratedDatasets.get(k).addTask(fTasks.get(i));
    					//adjust tasks
    					fTasks.get(i).removeInput(fGeneratedDatasets.get(k));
    					fTasks.get(i).addInput(x);
    					//Log.print("task="+fTasks.get(i)+" ");
    					//fDatasets.get(k).afficheDataset(x);
    				}
    		}
    		
    		//adjust output tasks
    		for(int j=0;j<fTasks.get(i).getOutput().size();j++)
            {
        		DataSet x = fTasks.get(i).getOutput().get(j);
    			
        		for(int k=0;k<fDatasets.size();k++)
    				if((fDatasets.get(k).getName().equals(x.getName()))&&(fDatasets.get(k).getSize()==x.getSize()))
    				{
    					fTasks.get(i).removeOutput(x);
    					fTasks.get(i).addOutput(fDatasets.get(k));
    				}
    			
    			for(int k=0;k<fGeneratedDatasets.size();k++)
    				if((fGeneratedDatasets.get(k).getName().equals(x.getName()))&&(fGeneratedDatasets.get(k).getSize()==x.getSize()))
    				{
    					fTasks.get(i).removeOutput(x);
    					fTasks.get(i).addOutput(fGeneratedDatasets.get(k));
    				}
    		}
        }
    }
    
    public ArrayList<DataSet> generated2()
    {
        ArrayList<DataSet> d = new ArrayList<DataSet>();
        for(int i=0;i<fDatasets.size();i++)
        {
        	 if(fDatasets.get(i).wasGenerated()==true) {
        		 //d.add(fDatasets.get(i)); 
        		 DataSet x = new DataSet();
        		 x.setExists(true);x.setName(fDatasets.get(i).getName());x.setSize(fDatasets.get(i).getSize());
        		 d.add(x);
             }
        	 
        	
        }
    	return d;
    }
    
    public ArrayList<DataSet> generated(ArrayList<DataSet> fDatasets)
    {
    	ArrayList<DataSet> gdata = new ArrayList<DataSet>() ;
    	final Map<String, DataSet> generated;
    	generated = new HashMap<>();
        
    	for(int i=0;i<fDatasets.size();i++)
        {
    		generated.put(fDatasets.get(i).getName()+fDatasets.get(i).getSize(), fDatasets.get(i));
        }
    	
        Set cles = generated.keySet();
        Iterator it = cles.iterator();
        while (it.hasNext()){
           Object cle = it.next(); 
           Object valeur = cle.toString(); 
           
           DataSet dataset = (DataSet) generated.get(valeur);
           if(dataset.wasGenerated()==true) {
        	  gdata.add(dataset); 
           }
        }
        return gdata;
    }
    
    //get initial datasets
    public ArrayList<DataSet> initialDatasets(ArrayList<DataSet> fDatasets, ArrayList<DataSet> fGeneratedDatasets2)
    {
    	ArrayList<DataSet> fdata = new ArrayList<DataSet>() ;
    	final Map<String, DataSet> all;
    	all = new HashMap<>();
   
    	//supprimer les doublons de tous les datasets
    	for(int i=0;i<fDatasets.size();i++)
        {
        	all.put(fDatasets.get(i).getName()+fDatasets.get(i).getSize(), fDatasets.get(i));
        }
    	
    	//delete generated datasets
    	for(int i=0;i<fGeneratedDatasets2.size();i++)
        {
    		Object val = fGeneratedDatasets2.get(i).getName()+fGeneratedDatasets2.get(i).getSize();
    		Object elesup = all.remove(val);
        }
    	
		Set cles = all.keySet();
    	Iterator it = cles.iterator();
    	while (it.hasNext()){
    		Object cle = it.next(); 
    		String valeur = cle.toString(); 
    		DataSet dataset = (DataSet) all.get(valeur);
    		fdata.add(dataset);
    	}

    	return fdata;	
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
       
    public void afficheDatasets(ArrayList<DataSet> fdata)
   	{
   		for(int i=0;i<fdata.size();i++)
           {
           	Log.print("Dataset:"  +fdata.get(i).getName());
           	Log.print(" Size:"  + fdata.get(i).getSize());
           	Log.print(" Generated:"  + fdata.get(i).wasGenerated());
           	Log.printLine();
           }
   	}
       
    public void afficheDataset(DataSet fdata)
   	{
   		Log.print("Dataset:"  +fdata.getName());
           Log.print(" Size:"  + fdata.getSize());
           Log.print(" Generated:"  + fdata.wasGenerated());
           Log.printLine();
   	}
      
}