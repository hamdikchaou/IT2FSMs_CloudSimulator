package IT2FCMPSO;

import net.sourceforge.jswarm_pso.Particle;

/**
 * The PSO Particle Class.
 * @author Hamdi Kchaou
 * @date March 12, 2021
 */
public class MyParticle extends Particle {

	/** Number of dimentions for this particle */
	 static int NUMBER_OF_DIMENTIONS = 2 ;
	
	
	public MyParticle() {
		super(NUMBER_OF_DIMENTIONS); // Create a 3-dimentional particle

	}

}
