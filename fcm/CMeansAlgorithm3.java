package fcm;

import java.util.HashMap;
import java.util.Map;

public class CMeansAlgorithm3 {
    private static int fuzzyness = 2;

    private final Map<Double, Species> integerClusterHashMap = new HashMap<Double, Species>();

    /// Array containing all points used by the algorithm
    private List<Job> points;

    /// Gets or sets membership matrix
    public double[][] U;

    /// Algorithm precision
    private double eps = Math.pow(10, -5);

    /// Gets or sets objective function
    private double J;

    /// Gets or sets log message
    public String log;

    private List<Species> clusterList;

    public CMeansAlgorithm3(List<Job> points, int clusterSize){
        this.points = points;
        clusterList = initialiseCentroids(points, clusterSize);
        U = new double[points.size()][clusterList.size()];
        calculateClusterMembershipValues();
        recalculateClusterIndexes();
    }

    private void calculateClusterMembershipValues() {
        // Iterate through all points to create initial U matrix
        for (int i = 0; i < points.size(); i++) {
            Job p = points.get(i);
            double sum = 0.0;

            for (int j = 0; j < clusterList.size(); j++) {
                Cluster c = clusterList.get(j);
                double diff = Math.sqrt(Math.pow(p.getMidpointX() - c.getCentroid().getX(), 2.0) + Math.pow(p.getMidpointY() - c.getCentroid().getY(), 2.0));
                U[i][j] = (diff == 0) ? eps : diff;
                sum += U[i][j];
             }

             double sum2 = 0.0;
             for (int j = 0; j < clusterList.size(); j++) {
                 U[i][j] = 1.0 / Math.pow(U[i][j] / sum, 2.0 / (fuzzyness - 1.0));
                sum2 += U[i][j];
             }

             for (int j = 0; j < clusterList.size(); j++) {
                U[i][j] = U[i][j] / sum2;
             }    
        }
   }

   /// Recalculates cluster indexes
   private void recalculateClusterIndexes() {
        for (int i = 0; i < points.size(); i++) {
            double max = -1.0;
            Job p = points.get(i);

            for (int j = 0; j < clusterList.size(); j++) {
                max = U[i][j] > max ? U[i][j] : max;
//              if (max < U[i][j]) {
//                    max = U[i][j];
//                    p.setClusterIndex((max == 0.5) ? 0.5 : j);
//              }
            }
            p.setClusterIndex(max);
        }
    }

    /// Perform a complete run of the algorithm until the desired accuracy is achieved.
    /// For demonstration issues, the maximum Iteration counter is set to 20.
    /// Algorithm accuracy
    /// The number of steps the algorithm needed to complete
    public List<Species> run(double accuracy) {
        int k = 0;
        int maxIterations = 100;

        do {
            k++;
            J = calculateObjectiveFunction();
            calculateClusterCentroids();
            step();
            double Jnew = calculateObjectiveFunction();

            if (Math.abs(J - Jnew) < accuracy) break;
        }
        while (maxIterations > k);

        assignJobsToClusters();
        return clusterList;
    }

    /// Calculate the objective function
    /// The objective function as double value
    private double calculateObjectiveFunction() {
        double Jk = 0;

        for (int i = 0; i < this.points.size();i++) {
            for (int j = 0; j < clusterList.size(); j++) {
                Jk += Math.pow(U[i][j], this.fuzzyness) * Math.pow(this.calculateEuclidDistance(points.get(i), clusterList.get(j)), 2);
            }
        }
        return Jk;
    }

    private List<Species> initialiseCentroids(final List<Job> dataSet, final int speciesSize) {
        final List<Species> clusterList = new ArrayList<Species>();
        final List<Integer> uniqueIndexes = ToolBox.uniqueIndexes(dataSet.size(), speciesSize);

        for (int i=0; i< uniqueIndexes.size(); i++){
            final int randomIndex = uniqueIndexes.get(i);
            final Species species = new Species(i);
            final Centroid centroid = new Centroid(dataSet.get(randomIndex).getMidpointX(), dataSet.get(randomIndex).getMidpointY(), i);
            species.setCentroid(centroid);
            speciesList.add(species);
        }
        return clusterList;
    }

    /// Perform one step of the algorithm
    public void step() {
        for (int c = 0; c < clusterList.size(); c++) {
            for (int h = 0; h < points.size(); h++) {
                double top;
                top = calculateEuclidDistance(points.get(h), clusterList.get(c));
                if (top < 1.0) top = eps;

                // sumTerms is the sum of distances from this data point to all clusters.
                double sumTerms = 0.0;

                for (int ck = 0; ck < clusterList.size(); ck++) {
                    double thisDistance = calculateEuclidDistance(points.get(h), clusterList.get(ck));
                    if (thisDistance < 1.0) thisDistance = eps;
                    sumTerms += Math.pow(top / thisDistance, 2.0 / (fuzzyness - 1.0));

                }
                // Then the membership value can be calculated as...
                U[h][c] = (1.0 / sumTerms);
            }
        }

        recalculateClusterIndexes();
    }

    /// Calculates Euclid distance between point and centroid
    /// Point
    /// Centroid
    /// Calculated distance
    private double calculateEuclidDistance(Job p, Species c) {
        return ToolBox.calculateDistance(p.getMidpointX(), p.getMidpointY(), c.getCentroid().getX(), c.getCentroid().getY());
    }

    /// Calculates the centroids of the clusters
    private void calculateClusterCentroids() {
        for (int j = 0; j < clusterList.size(); j++) {
            Species c = clusterList.get(j);
            double uX = 0.0;
            double uY = 0.0;
            double membershipSum = 0.0;

            for (int i = 0; i < points.size(); i++) {
                Job p = points.get(i);

                double uu = Math.pow(U[i][j], this.fuzzyness);
                uX += uu * p.getMidpointX();
                uY += uu * p.getMidpointY();
                membershipSum += uu;
            }

            c.setMembershipSum(membershipSum);
            c.getCentroid().setX(((uX / membershipSum)));
            c.getCentroid().setY(((uY / membershipSum)));

            log += String.format("Cluster Centroid: (" + c.getCentroid().getX() + "; " + c.getCentroid().getY() + ")");
        }
    }

    private void assignJobsToClusters(){
        for (final Cluster cluster : clusterList){
            if (!integerClusterHashMap.containsKey(cluster.getMembershipSum()))
                integerClusterHashMap.put(cluster.getMembershipSum(), cluster);
        }

        for (Job job : points){
            final double clusterIndex = job.getClusterIndex();
            Species c = integerSpeciesHashMap.get(clusterIndex);

            if (c != null) {
                c.add(job);
            }
        }
    }