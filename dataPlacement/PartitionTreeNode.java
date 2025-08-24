/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package dataPlacement;

import java.util.ArrayList;
import java.util.HashSet;

/**
 *
 * @author Bryce
 */
public class PartitionTreeNode implements Comparable {
    ArrayList<DataSet> fDataSet;
    protected PartitionTreeNode lChild, rChild, fParent, fSibling;
    private boolean assigned;
    private boolean hasChildren;
    PartitionTree fTree;
    int size;
    int fixedSize;
    private int fHorizontalPosition;
    ArrayList<DataCenter> myHome;
    private DataCenter myDataCentre;
    public int dependancyValue = 0;
    public HashSet<PartitionTreeNode> myChildren;
    private Clusterer fCluster = new BEA();


//    public PartitionTreeNode() {
//        fDataSet = new ArrayList<DataSet>();
//        myDataCentre = new  DataCenter();
//    }

    public PartitionTreeNode(ArrayList<DataSet> aDataSet, PartitionTree aTree) throws DistributionException {
        this(aDataSet, null, aTree, 0);
    }

    protected PartitionTreeNode(ArrayList<DataSet> fDataSet, PartitionTreeNode aParent, PartitionTree aTree, int aHorizontalPosition) throws DistributionException{
        System.out.println("Making a new node " + fDataSet.size());
        myChildren = new HashSet<PartitionTreeNode>();
        myDataCentre = new DataCenter();
        fParent = aParent;
        fTree = aTree;
        fHorizontalPosition = aHorizontalPosition;
        this.fDataSet = new ArrayList<DataSet>(fDataSet);
        myHome = new ArrayList<DataCenter>();
        for(DataSet s : fDataSet){
            size += s.getSize();
            if(s.getFixedAddress() != null)
                fixedSize += s.getSize();
            if(s.getFixedAddress() != null && !myHome.contains(s.getFixedAddress()))
                myHome.add(s.getFixedAddress());
        }
        myHome.trimToSize();
        System.out.println(myHome.size() + "   " + fDataSet.size());
        try{
            if(myHome.size() > 1)
                split();
        }catch(DistributionException e){
            e.printStackTrace();
        }
        System.out.println("Returning " + fDataSet.size());
    }

    public boolean split() throws DistributionException{

        if(rChild != null || lChild != null)
            throw new DistributionException("Trying to split a node which already has children");
        if(this.fDataSet.size() == 1)
            return false;
//            throw new DistributionException("Trying to split a node which has only one dataset");
        ArrayList<DataSet> lChildData = new ArrayList<DataSet>();
        ArrayList<DataSet> rChildData = new ArrayList<DataSet>();
        int l = 0;
        Matrix lMatrix = new Matrix();

        int bestPoint = 0;
		int bestPointScore = 0;
        for(int i = 0; i < fDataSet.size(); i++)
            {
                lMatrix.addDataset(fDataSet.get(i));
            }
        lMatrix = fCluster.cluster(lMatrix);
        for(int i = 1; i < fDataSet.size() - 1; i++)
		{
			int thisPointScore = calculatePartitionPointScore(lMatrix, i);

			if(thisPointScore > bestPointScore)
			{
				bestPointScore = thisPointScore;
				bestPoint = i;
			}
		}
        for(int i = 0; i < bestPoint; i++){
            lChildData.add(lMatrix.getDatasets().get(i));
        }
        for(int i = bestPoint; i < lMatrix.getDatasets().size(); i++){
            rChildData.add(lMatrix.getDatasets().get(i));
        }
        if(this.fDataSet.size() == 2){
            lChildData = new ArrayList<DataSet>();
            lChildData.add(fDataSet.get(0));
            rChildData = new ArrayList<DataSet>();
            rChildData.add(fDataSet.get(1));
        }
        if(this.fDataSet.size() <= 1)
            throw new DistributionException("Trying to split a node that has one or less datasets");
        if(lChildData.size() == 0 || rChildData.size() == 0)
            throw new DistributionException("Trying to split a node such that it will have an empty child");
        lChild = new PartitionTreeNode(lChildData, this, fTree, this.fHorizontalPosition);
        rChild = new PartitionTreeNode(rChildData, this, fTree, this.fHorizontalPosition + 1);
        lChild.fSibling = rChild;
        rChild.fSibling = lChild;
        fTree.addLeaves(lChild, rChild);
        for(PartitionTreeNode p : fTree.fLeaves.values())
            System.out.println(p.toString() + " " + p.size + " " + p.getDataSets().size());
        this.hasChildren = true;
        PartitionTreeNode tmp = this.fParent;
        this.myChildren.add(lChild);
        this.myChildren.add(rChild);
        while ( tmp != null && tmp.fParent != null){
            tmp.myChildren.add(lChild);
            tmp.myChildren.add(rChild);
            tmp = tmp.fParent;
        } 
        return true;
    }

    /**
     * @return the horizontalPosition
     */
    public int getHorizontalPosition() {
        return fHorizontalPosition;
    }

    /**
     * @param horizontalPosition the horizontalPosition to set
     */
    public void setHorizontalPosition(int horizontalPosition) {
        this.fHorizontalPosition = horizontalPosition;
    }
    
    public ArrayList<DataSet> getDataSets(){
        return fDataSet;
    }

    public int compareTo(Object o) {
        if(o instanceof PartitionTreeNode)
            return this.size - ((PartitionTreeNode)o).size;
        throw new ClassCastException("Comparison failure with PartionionTreeNode!");
    }

    /**
     * @return the assigned
     */
    public boolean isAssigned() {
        return assigned;
    }

    /**
     * @param assigned the assigned to set
     */
    public void setAssigned(boolean assigned) {
        System.out.println(this.toString() + " has been assigned");
        this.assigned = assigned;
        if(assigned){
            if(fParent != null){
                if(fSibling.isAssigned())
                    fParent.setAssigned(true);
            }
        }
    }
    public boolean assign(DataCenter dc) throws Exception {
        if(this.isAssigned())
            throw new Exception("Trying to assign an already assigned node!");
        
        if(myHome.size() == 1)
            if(myHome.get(0) != dc)
                throw new Exception("Tried to assign to the wrong datacentre! " + myHome.get(0).getName() + " " + dc.getName());
        this.myDataCentre = dc;
        for(DataSet ds : fDataSet){
            ds.setDC(dc);
            if(dc.getDatasets().contains(ds))
                continue;
            dc.addDataset(ds);
        }
        setAssigned(true);
        myHome.add(dc);
        fParent.myHome.add(dc);
        myDataCentre = dc;
        System.out.println(this.toString() + " has assigned successfully");
        return true;
    }

    public PartitionTreeNode getGreatestDependency(){
        if(!fSibling.isAssigned())
            return fSibling;
        return fParent.getGreatestDependency();
    }

    /**
     * @return the hasChildren
     */
    public boolean hasChildren() {
        return hasChildren;
    }

    public DataCenter getMyDataCentre() {
        return myDataCentre;
    }

    public PartitionTreeNode getNearestUnassignedNode(HashSet<PartitionTreeNode> aNode){
        HashSet<PartitionTreeNode> newANode = new HashSet<PartitionTreeNode>(aNode);
        newANode.add(this);
        PartitionTreeNode result = null;
        if(lChild != null && !newANode.contains(lChild))
            result = lChild.getNearestUnassignedNode(newANode);
        if(result != null)
            return result;
        if(rChild != null && !newANode.contains(rChild))
            result = rChild.getNearestUnassignedNode(newANode);
        if(result != null)
            return result;
        if(!aNode.contains(this) && !this.isAssigned() && !this.hasChildren())
            return this;
        if(aNode.contains(fParent))
            return null;
        return fParent.getNearestUnassignedNode(newANode);

    }

    public int GetPartitionDistanceFromMe(PartitionTreeNode p){
        return fTree.getDistanceFromPartion(this, p);
    }
    public void fixFData(){
        if(fTree.fData.containsValue(this) && this.hasChildren()){
            for(DataSet ds : rChild.getDataSets())
                fTree.fData.put(ds, rChild);
            rChild.fixFData();
            for(DataSet ds : lChild.getDataSets())
                fTree.fData.put(ds, lChild);
            lChild.fixFData();
        }
    }
    public void fixFLeaves(){
        if(fTree.fLeaves.containsValue(this) && this.hasChildren()){
            fTree.fLeaves.remove(this);
            if(!rChild.hasChildren())
                if(!fTree.fLeaves.containsValue(rChild))
                    fTree.fLeaves.put(fHorizontalPosition, this);
            else
                rChild.fixFLeaves();
            if(!lChild.hasChildren())
                if(!fTree.fLeaves.containsValue(lChild))
                    fTree.fLeaves.put(fHorizontalPosition, this);
            else
                lChild.fixFLeaves();
        }
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
}
