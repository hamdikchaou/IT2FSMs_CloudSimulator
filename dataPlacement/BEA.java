package dataPlacement;

import java.util.Iterator;
import java.util.LinkedList;

public class BEA extends Clusterer
{
	
	@Override
	public Matrix cluster(Matrix aMatrix)
	{
		//just quickly check that there are more than two datasets,
		//if there isn't, it can't be optimised any more!
		if(aMatrix.getData().length <= 2)
			return aMatrix;
		LinkedList<DataSet> lNewMatrixOrder = new LinkedList<DataSet>();
		
		//initialise, add the first two datasets from the old matrix to the new one
		lNewMatrixOrder.add(aMatrix.getDatasets().get(0));
		lNewMatrixOrder.add(aMatrix.getDatasets().get(1));
		
		//demande bcp de temps en cas de grand nombre de datasets
		//for each of the other datasets
		for(int i = 2; i < aMatrix.getDatasets().size(); i++)
		{
			int bestIndex = findBestNewIndex(lNewMatrixOrder, i, aMatrix);		
			System.out.println("New Matrix"+lNewMatrixOrder.toString());
                        System.out.println("bestIndex=== "+bestIndex);
			//insert this dataset at the best location
			lNewMatrixOrder.add(bestIndex, aMatrix.getDatasets().get(i));
		}
		
		//create a new Matrix, populate it and return it
		Matrix result = new Matrix();
		//System.out.println(result.toString());
		Iterator<DataSet> lIterator =  lNewMatrixOrder.iterator();
		
		while(lIterator.hasNext())
			result.addDataset(lIterator.next());
		
		return result;
	}

	private int findBestNewIndex(LinkedList<DataSet> lNewMatrixOrder, int i, Matrix aMatrix)
	{
		//find the index where it would have the biggest contribution
		int bestIndex = 0;
		int bestCont = 0;
		
		//try the leftmost boundary (that is, left of column 0)
		int thisCont = boundaryContribution(lNewMatrixOrder.get(0), aMatrix.getDatasets().get(i), aMatrix);
		if(thisCont > bestCont)
		{
			bestIndex = 0;
			bestCont = thisCont;
		}
		
		//try the rightmost boundary (that is, right of column i)
		thisCont = boundaryContribution(lNewMatrixOrder.get(i - 1), aMatrix.getDatasets().get(i), aMatrix);
		if(thisCont > bestCont)
		{
			bestIndex = i;
			bestCont = thisCont;
		}
		
		for(int j = 1; j < i; j++)
		{
			thisCont = contribution(lNewMatrixOrder.get(j-1), 
									aMatrix.getDatasets().get(i), 
									lNewMatrixOrder.get(j), 
									aMatrix); 
			if(thisCont > bestCont)
			{
				bestIndex = j;
				bestCont = thisCont;
			}
		}
		return bestIndex;
	}
	
	private int contribution(DataSet aLeft, DataSet aNew, DataSet aRight, Matrix aMatrix)
	{
		int result = 2 * bond(aLeft, aNew, aMatrix);
		result += 2 * bond(aNew, aRight, aMatrix);
		
		result -= 2 * bond(aLeft, aRight, aMatrix);
		
		return result;
	}
	
	private int boundaryContribution(DataSet aBoundary, DataSet aNew, Matrix aMatrix)
	{
		return 2 * bond(aBoundary, aNew, aMatrix);
	}
	
	private int bond(DataSet d1, DataSet d2, Matrix aMatrix)
	{
		int result = 0;
		//get the indices of the two datasets in the matrix
		int d1Index = aMatrix.getDatasets().indexOf(d1);
		int d2Index = aMatrix.getDatasets().indexOf(d2);
		
		for(int i = 0; i < aMatrix.getData().length; i++)
		{
			result += aMatrix.getData()[i][d1Index] * aMatrix.getData()[i][d2Index];
		}
		
		return result;
	}
}
