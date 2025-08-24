package realworkflow;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.Collection;


public class PSO {

    public static double fitness(double[] x){
        double retValue = 0;

        for(int i=0; i<x.length; i++){
            retValue = retValue + Math.pow(x[i], 2);            
        }

        return retValue;

    }
    public static void main(String[] args){
        double w         = 0.9, 
               c1        = 2.05,  c2        = 2.05, 
               r1        = 0.0,   r2        = 0.0,
               xMin      = -5.12, xMax      = 5.12,
               vMin      = 0,     vMax      = 1,
               wMin      = 0.4,   wMax      = 0.9,
               phi       = c1+c2,
               chi       = 2.0/Math.abs(2.0-phi-Math.sqrt(Math.pow(phi, 2)-4*phi)),
               nInfinite = Double.NEGATIVE_INFINITY,
               gBestValue = nInfinite;
        
        int   Np = 100,  // # of particles
              Nd = 2,   // # of dimensions
              Nt = 1000; // # of Time Steps
        
        double[] pBestValue         = new double[Np],
                 gBestPosition      = new double[Nd],
                 bestFitnessHistory = new double[Nt],
                 M                  = new double[Np];

        double[][] pBestPosition = new double[Np][Nd],
                   R             = new double[Np][Nd],
                   V             = new double[Np][Nd];
        
        Random rand = new Random();
            
        for(int p=0; p<Np; p++){
            pBestValue[p] = nInfinite; 
        }

        for(int p=0; p<Np; p++){  
            for(int i=0; i<Nd; i++){
                R[p][i] = xMin + (xMax-xMin)*rand.nextDouble();
                V[p][i] = vMin + (vMax-vMin)*rand.nextDouble();

                if(rand.nextDouble() < 0.5){
                    V[p][i] = -V[p][i];
                    R[p][i] = -R[p][i];
                }
            }
        }
    
        for(int p=0; p<Np; p++){
            M[p] = fitness(R[p]);
            M[p] = -M[p];
        }
        
        for(int j=0; j<Nt; j++){ 
            for(int p=0; p<Np;p++){         
                for(int i=0; i<Nd; i++){    
                    R[p][i] = R[p][i] + V[p][i];

                    if(R[p][i] > xMax)          { R[p][i] = xMax;}
                    else if(R[p][i] < xMin)     { R[p][i] = xMin;}
                }           
            }   

            for(int p=0; p<Np; p++){ 

                M[p] = fitness(R[p]);
                M[p] = -M[p];
            
                if(M[p] > pBestValue[p]){
                
                     pBestValue[p] = M[p];
                     for(int i=0; i<Nd; i++){
                        pBestPosition[p][i] = R[p][i];
                     }
                 }
            
                if(M[p] > gBestValue){
        
                    gBestValue = M[p];          
                    for(int i=0; i<Nd; i++){
                       gBestPosition[i] =  R[p][i];
                    }
                }
            
            }
            bestFitnessHistory[j] = gBestValue;
        
            w = wMax - ((wMax-wMin)/Nt) * j;
            for(int p=0; p<Np; p++){
                for(int i=0; i<Nd; i++){
                    
                    r1 = rand.nextDouble();
                    r2 = rand.nextDouble();
                    V[p][i] = chi * w * (V[p][i] + r1 * c1 * (pBestPosition[p][i] - R[p][i]) + r2*c2 *(gBestPosition[i] - R[p][i]));
                
                    // classic Velocity update formulate                
                    if      (V[p][i] > vMax) { V[p][i] = vMax; }        
                    else if (V[p][i] < vMin) { V[p][i] = vMin; }
                }
            }
            //output global best value at current timestep
            System.out.println("iteration: " + j + " BestValue " + gBestValue);
        }   
    }
}
