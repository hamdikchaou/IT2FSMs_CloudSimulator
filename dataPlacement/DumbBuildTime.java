package dataPlacement;

import java.util.ArrayList;

import workflow.Workflow;

public class DumbBuildTime extends BuildTimeAlgorithm
{
    boolean ENABLE_FIXED_DATASETS = true;
	public DumbBuildTime(ArrayList<DataCenter> aDatacenters)
	{
		super(aDatacenters);
	}
	
	public void setDependancyMatrix(Matrix aMatrix)
	{
		//do nothing, matrices aren't used here!
	}
	
	public ArrayList<DataCenter> distribute(Workflow aWorkflow) throws DistributionException
	{
		return distribute(aWorkflow.getDatasets());
	}
	
	public ArrayList<DataCenter> distribute(ArrayList<DataSet> aDatasets) throws DistributionException
	{
        for(int i = 0; i < fDataCenters.size(); i++)
			fDataCenters.get(i).getDatasets().clear();
		
        if(ENABLE_FIXED_DATASETS){
            for(DataSet ds : aDatasets){
                try{
                    if(ds.getFixedAddress() != null){
                        ds.getFixedAddress().addDataset(ds);
                        ds.setDC(ds.getFixedAddress());
                    }
                } catch(Exception e)
                {
                    throw new DistributionException("No space for fixed data!");
                }
            }
        }

		ArrayList<DataCenter> result = new ArrayList<DataCenter>();
		for(int i = 0; i < aDatasets.size(); i++)
		{

            if(ENABLE_FIXED_DATASETS && aDatasets.get(i).getFixedAddress() != null){
                if(!result.contains(aDatasets.get(i).getFixedAddress()))
					result.add(aDatasets.get(i).getFixedAddress());
                continue;
            }
			//pick a random datacenter for it
			DataCenter lTargetCenter = fDataCenters.get(fRandom.nextInt(fDataCenters.size()));
			//add it if you can
			try
			{
				lTargetCenter.addDataset(aDatasets.get(i));
				aDatasets.get(i).setDC(lTargetCenter);
				if(!result.contains(lTargetCenter))
					result.add(lTargetCenter);
			}
			catch(Exception ex)
			{
				//it can't fit
				i--;
				//try this set again
				continue;
			}
		}
		
		//now only return the datacenters that are used
		return result;
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
