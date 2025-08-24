/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package dataPlacement;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflow.Workflow;

/**
 *
 * @author Bryce
 */
public class TreeBuildTimeNearestSize extends BuildTimeAlgorithm {
HashMap<DataCenter, Double> space;
PartitionTree fTree;
private Clusterer fCluster = new BEA();
    public TreeBuildTimeNearestSize(ArrayList<DataCenter> aDatacenters)
	{
		super(aDatacenters);
	}


    @Override
    public ArrayList<DataCenter> distribute(Workflow aWorkflow) throws DistributionException {
        return distribute(aWorkflow.getDatasets());
    }

    @Override
    public ArrayList<DataCenter> distribute(ArrayList<DataSet> aDatasets) throws DistributionException {
        resetDataCenters();
        System.out.println("Starting Tree Nearest Size distribution");
        space = new HashMap<DataCenter,Double>();
        //Hack up some "fixed" datacentre business.
        preOrganiseFixedData(aDatasets);
        //Start the real algorithm.
        fTree = new PartitionTree(aDatasets, fDataCenters);
        sortFixedData(aDatasets); //sort fixed datasets before going any further

        while(!fTree.distributionCompleted()){
            doTheRealWork(aDatasets);
        }
        for(DataCenter d : fDataCenters)
            d.setMaxCapacity(true);
        return fDataCenters;
    }

    @Override
    public void setDependancyMatrix(Matrix aMatrix) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private int calculatePartitionPointScore(Matrix aMatrix, int aPoint)
	{
		int topLeft = 0;

		for(int i = 0; i <= aPoint; i++)
			for(int j = 0; j <= aPoint; j++)
				topLeft += aMatrix.getData()[i][j];

		int bottomRight = 0;
		for(int i = aPoint + 1; i < aMatrix.getData().length; i++)
			for(int j = aPoint + 1; j < aMatrix.getData().length; j++)
				bottomRight += aMatrix.getData()[i][j];

		int excludedPoints = 0;
		for(int i = 0; i <= aPoint; i++)
			for(int j = aPoint + 1; j < aMatrix.getData().length; j++)
				excludedPoints += aMatrix.getData()[i][j];

		int result = topLeft * bottomRight;

		result -= Math.pow(excludedPoints, 2);

		return result;
	}

    public void preOrganiseFixedData(ArrayList<DataSet> aDatasets){
        for(DataCenter d : fDataCenters){
            space.put(d, d.freeSpace());
            d.setDatasets(new ArrayList<DataSet>());
        }
        for(DataSet ds : aDatasets) {
            if(ds.getFixedAddress() != null){
                ds.getFixedAddress().setHasFixedData(true); //make sure every datacentre knows it has data it must host
                try {
                    if(space.get(ds.getFixedAddress()) == null)
                        space.put(ds.getFixedAddress(), ds.getFixedAddress().freeSpace() ); //make sure there'll be space
                    if(space.get(ds.getFixedAddress()) - ds.getSize() > 0)
                        space.put(ds.getFixedAddress(), space.get(ds.getFixedAddress()) - ds.getSize());
                    else
                        throw new DistributionException("Impossible amount of space required for fixed data.");
                    //ds.getFixedAddress().addDataset(ds);
                    System.out.println(ds.getName() + " added to " + ds.getFixedAddress().getName() + " leaving " + space.get(ds.getFixedAddress()) + " worth of free space tree");
                    //ds.getFixedAddress().addDataset(ds);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

        }
        for(DataCenter d : fDataCenters)
            System.out.println(d.getName() + " has " + space.get(d) + " space left after required space for fixed datasets tree");
    }

    public void sortFixedData(ArrayList<DataSet> aDatasets)throws DistributionException{
        int q = 0;
        for(DataCenter dc : fDataCenters)
            dc.setDatasets(new ArrayList<DataSet>());
            
//            if(!dc.hasFixedData())  //this datacentre has no fixed data.
//                continue;
        int oldI = 0, loopCatch = 0;
        for(int i = 0; i < aDatasets.size(); i++){ //for each fixed datacentre that the data centre needs to contain.
            if(oldI == i)
                loopCatch++;
            else
                loopCatch = 0;
            oldI = i;
            if(loopCatch > 50)
                break;
            if(aDatasets.get(i).getFixedAddress() == null)
                continue;

            System.out.println("Sorting out fixed data.");
            PartitionTreeNode lPartition = fTree.getPartition(aDatasets.get(i)); // get the partition the data set belongs to
            if(lPartition.hasChildren()){ //if the partition retrieved has children then something has gone wrong as the get above should only know about leaves.
                System.out.println("Has children, continuing.");
                lPartition.fixFData(); //fix the data structure used above to have the correct location of this data set
                i--; //try this partion again
                continue;
            }
            if(lPartition.isAssigned()){ //if this dataset has already been assigned then we don't need to do it again.
                System.out.println("Already assigned, continuing.");
                continue;
            }
            System.out.println("Trying to fit " + " " + lPartition.size + " " + lPartition.fixedSize + " into a " + aDatasets.get(i).getFixedAddress().freeSpace() + " sized space "  + space.get(aDatasets.get(i).getFixedAddress()) + " " + aDatasets.get(i).getFixedAddress().getName());
            if(((lPartition.size - lPartition.fixedSize) <= space.get(aDatasets.get(i).getFixedAddress()) ) ){ //if the datacentre has enough space for me, and will still have enough space for the rest of its fixed data sets.
                try {
                    System.out.println(lPartition.toString() + " is trying to assign to fixed dc " + aDatasets.get(i).getFixedAddress().getName());
                    lPartition.assign(aDatasets.get(i).getFixedAddress()); //assign it to the datacentre
                    space.put(aDatasets.get(i).getFixedAddress(), space.get(aDatasets.get(i).getFixedAddress()) - (lPartition.size -lPartition.fixedSize)); // "space" keeps a record of how much space will be needed to fit fixed data in each data set. update it to reflect this change.
                    } catch (Exception ex) {
                        i--;
                    }
            } else {
                    System.out.println("Splitting a partition.");
                    for(DataSet s : aDatasets.get(i).getFixedAddress().getDatasets()){
                        PartitionTreeNode current = fTree.getPartition(s);
                        if(current.isAssigned())
                            continue;
                        if(current.hasChildren()){
                            current.fixFData();
                            continue;
                        }
                        if(current.getDataSets().size() <= 1)
                            continue;
                        if(current.size > lPartition.size)
                            lPartition = current;
                    }
                    System.out.println(lPartition.toString() + " should be splitting");
                    lPartition.split();
                    i--;
            }
            
        }
        for(PartitionTreeNode p : fTree.fLeaves.values()){
            if(!p.isAssigned() && p.myHome.size() > 0)
                if(p.hasChildren())
                    throw new DistributionException("Fixed data allocation error! " + p.toString() + " " + p.size + " this was caused by a leaf/child error...");
                else {
                    System.out.println("Fixed data allocation error! ");
                    System.out.println(p.myHome.get(0).getName() + " " + p.myHome.get(0).freeSpace() );
                    for(DataSet d : p.myHome.get(0).getDatasets())
                        System.out.println("\t" + d.getName() );
                    System.out.println(p.toString() + " should contain " );
                    for(DataSet d : p.getDataSets()){
                        System.out.println("\t" + d.getName() );
                    }
                    throw new DistributionException("Fixed data allocation error! " + p.toString() + " " + p.size);
                }
        }
    }
    public void doTheRealWork(ArrayList<DataSet> aDatasets) throws DistributionException {
        for(PartitionTreeNode p : fTree.fLeaves.values())
                if(!p.isAssigned())
                    System.out.println(p.toString() + " hasn't been assigned... Nearest");

            PartitionTreeNode lGreatestDep = findTheGreatestDep(aDatasets); //get the unassigned patition with the greatest dependancy value
            if(lGreatestDep == null){ //if the function above didn't work it means there's only one unassigned dataset remaining, this is failsafe fallback to make sure we have something to partition
                for(PartitionTreeNode p : fTree.fLeaves.values())
                    if(!p.isAssigned())
                        lGreatestDep = p;
            }
            if(lGreatestDep == null)
                return; //if the greatet dependancy is still null then distribution has finished but loop didn't exit last time. Return so that we can break or it will try again.
            DataCenter greatestSpace = null;
            for(DataCenter d : fDataCenters){ //get the datacentre with the most free space.
                System.out.println("DataCentre " + d.getName() + " " + d.freeSpace());
                if( greatestSpace == null || greatestSpace.freeSpace() < d.freeSpace())
                    greatestSpace = d;
            }
            System.out.println("Greatest datacentre free space " + greatestSpace.getName() + " " + greatestSpace.freeSpace());
            System.out.println("Greatest dep size " + lGreatestDep.toString() + " " + lGreatestDep.size + " " + lGreatestDep.isAssigned() + " " + lGreatestDep.myHome.size() );
            if(lGreatestDep.size <= greatestSpace.freeSpace()){
                if(!lGreatestDep.fSibling.isAssigned()) { //My sibling hasn't been assigned. First try empty datacentres then find the datacentre with free space that's the closest to my size
                    mySiblingHasntBeenAssigned(greatestSpace, lGreatestDep);

                } else if(lGreatestDep.fSibling.getMyDataCentre().freeSpace() >= lGreatestDep.size) { //my sibling has space for me :) assign there
                    try {
                        lGreatestDep.assign(greatestSpace);
                    } catch (Exception ex) {
                        Logger.getLogger(TreeBuildTimeNearestSize.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    mySiblingHasBeenAssigned(greatestSpace, lGreatestDep); //my sibling has been assigned. 
                }
            } else {//too big to fit anywhere. split until i do
                if(lGreatestDep.size > greatestSpace.freeSpace())
                    while(lGreatestDep.size > greatestSpace.freeSpace()){
                        if(lGreatestDep.getDataSets().size() == 1)
                            throw new DistributionException("Trying to partition a single dataset...");
                        lGreatestDep.split();
                        if(lGreatestDep.lChild.getDataSets().size() > lGreatestDep.rChild.getDataSets().size())
                            lGreatestDep = lGreatestDep.lChild;
                        else
                            lGreatestDep = lGreatestDep.rChild;
                    }
                try {
                    lGreatestDep.assign(greatestSpace);
                } catch (Exception ex) {
                    Logger.getLogger(TreeBuildTimeNearestSize.class.getName()).log(Level.SEVERE, null, ex);
                }
            }


    }
    public PartitionTreeNode findTheGreatestDep(ArrayList<DataSet> aDatasets) {
        Matrix lMatrix = new Matrix();
            for(PartitionTreeNode p : fTree.fLeaves.values())
                p.dependancyValue = 0;
            for(int i = 0; i < aDatasets.size(); i++)
            {
                lMatrix.addDataset(aDatasets.get(i));
            }
            lMatrix = fCluster.cluster(lMatrix);
                for(int i = 1; i < lMatrix.getDatasets().size(); i++)
            {
                int thisPointScore = calculatePartitionPointScore(lMatrix, i);
                Integer newValue = fTree.getPartition(aDatasets.get(i)).dependancyValue;
                newValue += thisPointScore;
                fTree.getPartition(aDatasets.get(i)).dependancyValue = newValue;
            }

            TreeSet<PartitionTreeNode> sortedDeps = new TreeSet<PartitionTreeNode>(new Comparator() {
                public int compare(Object o1, Object o2) {
                    return ((PartitionTreeNode)o1).dependancyValue - ((PartitionTreeNode)o2).dependancyValue;
                }
            });
            for(PartitionTreeNode p : fTree.fLeaves.values())
                sortedDeps.add(p);

            PartitionTreeNode lGreatestDep = sortedDeps.pollLast();
            System.out.println("Trying as greatest deps: " + lGreatestDep.toString() + " haschildren: " + lGreatestDep.hasChildren() + " ass " + lGreatestDep.isAssigned() );
            try{
                if(lGreatestDep.isAssigned()) //make sure the greatest dep we found isn't already assigned. Uses a sorted set by dependancy value
                while(lGreatestDep.isAssigned()){
                    System.out.println("Trying as greatest deps: " + lGreatestDep.toString() + " haschildren: " + lGreatestDep.hasChildren() + " ass " + lGreatestDep.isAssigned() );
                    lGreatestDep = sortedDeps.pollLast();
                }
            }catch (NullPointerException e) {
                return null;

            }catch (NoSuchElementException e) {
                return null;
            }
            return lGreatestDep;
    }
    public void mySiblingHasntBeenAssigned(DataCenter greatestSpace, PartitionTreeNode lGreatestDep){
        ArrayList<DataCenter> emptyDataCentres = new ArrayList<DataCenter>();
                    for(DataCenter dc : fDataCenters) //make a list of empty data centres and try them first
                        if(dc.getDatasets().size() == 0)
                            emptyDataCentres.add(dc);
                    if(emptyDataCentres.size() != 0){
                        greatestSpace = emptyDataCentres.get(0);
                        for(DataCenter dc : emptyDataCentres)
                            if(dc.freeSpace() > greatestSpace.freeSpace())
                                greatestSpace = dc;
                        if(greatestSpace.freeSpace() >= lGreatestDep.size)
                            try {
                            lGreatestDep.assign(greatestSpace);
                        } catch (Exception ex) {
                            Logger.getLogger(TreeBuildTimeNearestSize.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        //find the nearest datacentre to my size
                            System.out.println("Finding nearest sizewise");
                            TreeMap<Double, HashSet<DataCenter>> whereToTry = new TreeMap<Double, HashSet<DataCenter>>(); //datacentres to try sorted by their closeness to my size
                            HashSet<DataCenter> tmp;
                            for(DataCenter ds : fDataCenters) { //add all datacentres that have enough space for me to the to try list, sorted by closeness to my size
                                if(ds.freeSpace() >= lGreatestDep.size){
                                    if(whereToTry.get(ds.freeSpace() - lGreatestDep.size) == null)
                                        tmp = new HashSet<DataCenter>();
                                    else
                                        tmp = whereToTry.get(ds.freeSpace() - lGreatestDep.size);
                                    tmp.add(ds);
                                    whereToTry.put(ds.freeSpace() - lGreatestDep.size, tmp);
                                }
                            }
                            for(Double i : whereToTry.keySet()){ //try the datacentres by closeness in size
                                try{
                                    for(DataCenter d : whereToTry.get(i))
                                        lGreatestDep.assign(d);
                                }catch(Exception e) {}
                                if(lGreatestDep.isAssigned())
                                    break;
                        }
                    }
    }
    public void mySiblingHasBeenAssigned(DataCenter greatestSpace, PartitionTreeNode lGreatestDep){

                    ArrayList<DataCenter> emptyDataCentres = new ArrayList<DataCenter>(); //try empty data centres.
                    for(DataCenter dc : fDataCenters)
                        if(dc.getDatasets().size() == 0)
                            emptyDataCentres.add(dc);
                    if(emptyDataCentres.size() != 0){ //if there are empty datacentres, try them
                        greatestSpace = emptyDataCentres.get(0);
                        for(DataCenter dc : emptyDataCentres)
                            if(dc.freeSpace() > greatestSpace.freeSpace())
                                greatestSpace = dc;
                        System.out.println("Trying to fit " + lGreatestDep.size + " into " + greatestSpace.freeSpace());
                        if(greatestSpace.freeSpace() >= lGreatestDep.size)
                            try {
                            lGreatestDep.assign(greatestSpace);
                        } catch (Exception ex) {
                            Logger.getLogger(TreeBuildTimeNearestSize.class.getName()).log(Level.SEVERE, null, ex);
                        }

                    } else { //if there were no empty data centres then try and find the one closest to my size
                        //find the nearest sizewise
                        System.out.println("Finding nearest sizewise");
                        TreeMap<Double, HashSet<DataCenter>> whereToTry = new TreeMap<Double, HashSet<DataCenter>>();
                        HashSet<DataCenter> tmp;
                            for(DataCenter ds : fDataCenters) { //add every data centre to the whereToTry map ordered by how close to my size it is
                                if(ds.freeSpace() >= lGreatestDep.size){
                                    if(whereToTry.get(ds.freeSpace() - lGreatestDep.size) == null)
                                        tmp = new HashSet<DataCenter>();
                                    else
                                        tmp = whereToTry.get(ds.freeSpace() - lGreatestDep.size);
                                    tmp.add(ds);
                                    whereToTry.put(ds.freeSpace() - lGreatestDep.size, tmp);
                                }
                            }
                            for(Double i : whereToTry.keySet()){ //try everywhere that'll fit me ordered by size.
                                for(DataCenter d : whereToTry.get(i)){
                                    try{
                                        lGreatestDep.assign(d);
                                    }catch(Exception e) {}
                                if(lGreatestDep.isAssigned())
                                    break;
                                }
                                if(lGreatestDep.isAssigned())
                                    break;
                        }
                    }
    }
    private void resetDataCenters()
	{
		//reset all the datacenters
		for(int i = 0; i < fDataCenters.size(); i++)
		{
			fDataCenters.get(i).resetDataCenter();
		}
	}


	@Override
	public Matrix getDependancyMatrix() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public Matrix getDependancyMatrixClustered() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public ArrayList<DataCenter> distributeFuzzy(Matrix fuzzyMatrix) {
		// TODO Auto-generated method stub
		return null;
	}
}
