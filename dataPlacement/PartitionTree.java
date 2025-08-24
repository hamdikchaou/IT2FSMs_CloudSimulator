/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package dataPlacement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 *
 * @author Bryce
 */
public class PartitionTree {

    ArrayList<DataSet> fDataSet;
    ArrayList<DataCenter> fDataCentre;
    HashMap<Integer, PartitionTreeNode> fLeaves;
    PartitionTreeNode fTop;
    HashMap<DataSet, PartitionTreeNode> fData;

    public PartitionTree(ArrayList<DataSet> fDataSet, ArrayList<DataCenter> fDataCentre) throws DistributionException {
        this.fDataSet = fDataSet;
        this.fDataCentre = fDataCentre;
        this.fLeaves = new HashMap<Integer, PartitionTreeNode>();
        fData = new HashMap<DataSet, PartitionTreeNode>();
        this.fTop = new PartitionTreeNode(fDataSet, this);
        if(!fTop.hasChildren())
            fLeaves.put(fTop.getHorizontalPosition(), fTop);
        if (fLeaves.containsValue(fTop) && fTop.hasChildren())
            for(Integer key : fLeaves.keySet())
                if(fLeaves.get(key) == fTop){
                    fLeaves.remove(key);
                    break;
                }
        for(DataSet ds : fDataSet) {
            if(fData.get(ds) == null)
                fData.put(ds, fTop);
        }
    }
    public boolean addLeaves(PartitionTreeNode aNode, PartitionTreeNode anotherNode){
        if(fLeaves.containsValue(aNode.fParent))
            for(Integer key : fLeaves.keySet())
                if(fLeaves.get(key) == aNode.fParent){
                    fLeaves.remove(key);
                    break;
                }
        if(fLeaves.containsValue(anotherNode.fParent))
            for(Integer key : fLeaves.keySet())
                if(fLeaves.get(key) == anotherNode.fParent){
                    fLeaves.remove(key);
                    break;
                }
        PartitionTreeNode tmp;
        if (!aNode.hasChildren()) {
            tmp = fLeaves.put(aNode.getHorizontalPosition(), aNode);
            while (tmp != null) {
                tmp.setHorizontalPosition(tmp.getHorizontalPosition() + 1);
                tmp = fLeaves.put(tmp.getHorizontalPosition(), tmp);
            }
            for (DataSet ds : aNode.getDataSets()) {
                fData.put(ds, aNode);
            }
        }
        if (!anotherNode.hasChildren()) {
            tmp = fLeaves.put(anotherNode.getHorizontalPosition(), anotherNode);
            while (tmp != null) {
                tmp.setHorizontalPosition(tmp.getHorizontalPosition() + 1);
                tmp = fLeaves.put(tmp.getHorizontalPosition(), tmp);
            }
            for (DataSet ds : anotherNode.getDataSets()) {
                fData.put(ds, anotherNode);
            }
        }

        
        return true;
    }
    public PartitionTreeNode getPartition(DataSet ds){
        if(fData.get(ds).hasChildren()){
            for(DataSet d : fData.get(ds).rChild.getDataSets())
                if(d == ds)
                    fData.put(d, fData.get(ds).rChild);
            for(DataSet d : fData.get(ds).lChild.getDataSets())
                if(d == ds)
                    fData.put(d, fData.get(ds).rChild);
        }
        return fData.get(ds);
    }


    public boolean split(PartitionTreeNode node) throws DistributionException{
        node.split();
        return true;
    }

    public boolean distributionCompleted(){
        return fTop.isAssigned();
    }

    public PartitionTreeNode getGreatestDependency(DataSet ds){
        return getGreatestDependency(getPartition(ds));
    }

    public PartitionTreeNode getGreatestDependency(PartitionTreeNode p){
        return p.getGreatestDependency();
    }

    public int getDistanceFromPartion(PartitionTreeNode pa, PartitionTreeNode pb){
        int result = 0;
        int paIndex = 0;
        int pbIndex = 0;
        for(Integer i : fLeaves.keySet()){
            if( fLeaves.get(i).equals(pb) )
                pbIndex = i;
            if( fLeaves.get(i).equals(pa) )
                paIndex = i;
        }
        if(paIndex > pbIndex)
            result = paIndex - pbIndex;
        else
            result = pbIndex - paIndex;
        return result;
    }
    public int getNumberOfUnassignedNodes(){
        int i = 0;
        for(PartitionTreeNode p : fLeaves.values())
            if(!p.hasChildren() && !p.isAssigned())
                i++;
        return i;
    }
}
