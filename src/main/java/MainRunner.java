import java.io.IOException;

import scala.Array;
import titan.TitanImoprtPerfTest;
import utils.PerformanceTest;
import workloads.ConcurrentWorkLoad;
import workloads.GraphWorkLoad;
import workloads.RandomWorkLoad;
import OrientDB.OrientDBImportPerfTest;
import cassandra.CassandraPerformance;

import com.datastax.driver.core.Cluster;


public class MainRunner {
	
	public void runner(int loads, int type) throws IOException{
		//GraphWorkLoad gwl = new ConcurrentWorkLoad(loads);
		GraphWorkLoad gwl = new RandomWorkLoad(loads);
		//GraphWorkLoad gwl = new FullWorkLoad(loads);
		
		PerformanceTest test;
		if (type == 0)
			test =  new CassandraPerformance(gwl);
		else if (type == 1)
			test = new OrientDBImportPerfTest(gwl);
		else if (type == 2)
			test = new TitanImoprtPerfTest(gwl);
		else
			test = new CassandraPerformance(gwl);
		
		long start = System.currentTimeMillis();
		test.run();
		System.out.println(test.getTestName() + ": " + (System.currentTimeMillis() - start));
		
		
	}
	public static void main(String[] args) throws IOException {
		int threads = Integer.parseInt(args[0]);
		final int testType = Integer.parseInt(args[1]);
		final int writeLoadsPerProcess = Integer.parseInt(args[2]);
		
		if (testType == 0){
			//cassandra test needs to create schema first;
			CassandraPerformance.createSchema();
		}
		for (int i = 0; i < threads; i++){
			Thread t = new Thread(new Runnable(){
				public void run(){
					MainRunner run = new MainRunner();
					try {
						run.runner(writeLoadsPerProcess, testType);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
			t.start();
		}
		
	}

}
