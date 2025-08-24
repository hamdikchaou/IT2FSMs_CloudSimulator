    package IT2FCMPSO;

import net.sourceforge.jswarm_pso.Neighborhood;
import net.sourceforge.jswarm_pso.Neighborhood1D;
import net.sourceforge.jswarm_pso.Swarm;
import net.sourceforge.jswarm_pso.example_2.SwarmShow2D;
public class PSOMain {

    /**
     * The main function
     */
    public static void main(String[] args) throws Exception  {

        int minPos = 1;
        int MaxPos = getVmList().size();

        int maxParticle = getTaskList().size(); 

        MyParticle.NUMBER_OF_DIMENTIONS = maxParticle;

        Swarm swarm = new Swarm(Swarm.DEFAULT_NUMBER_OF_PARTICLES, new MyParticle(),
                new MyFitnessFunction(getTaskList(), getVmList()));

        // Use neighborhood
        Neighborhood neigh = new Neighborhood1D(Swarm.DEFAULT_NUMBER_OF_PARTICLES / 5, true);
        swarm.setNeighborhood(neigh);
        swarm.setNeighborhoodIncrement(0.9);

        // Set position (and velocity) constraints. I.e.: where to look for solutions
        swarm.setInertia(0.9);
        swarm.setMaxPosition(MaxPos);
        swarm.setMinPosition(minPos);
        swarm.setMaxMinVelocity(0.5);

        int numberOfIterations = 100;
        boolean showGraphics = false;

        if (showGraphics) {
            int displayEvery = numberOfIterations / 100 + 1;
            SwarmShow2D ss2d = new SwarmShow2D(swarm, numberOfIterations, displayEvery, true);
            ss2d.run();
        } else {
            // Optimize (and time it)
            for (int i = 0; i < numberOfIterations; i++) {
                swarm.evolve();
            }
        }

        double[] psoAnswer = swarm.getBestPosition();

        int i = 0;
        for (Task task : getTaskList()) {
            int vmID = (int) Math.round(psoAnswer[i] - 1);
            CondorVM vm = (CondorVM) getVmList().get(vmID);
            task.setVmId(vm.getId());
            i++;
        }

        System.out.println(swarm.toStringStats());

    }
}
