package titan;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import utils.PerformanceTest;
import workloads.GraphWorkLoad;
import workloads.MetaEdge;
import cassandra.CassandraFullGraph;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;


public class TitanFullGraph {

	public static final String INDEX_NAME = "search";
	public TitanManagement tmanagement;
	public TitanGraph graph;
			
	private static String CONFIG_PATH="/Users/daidong/Downloads/titan.properties";
	
	public TitanFullGraph() {
	}
	
	public void connect(){
		graph=TitanFactory.open(CONFIG_PATH);
		tmanagement = graph.getManagementSystem();
	}
	
	public void load(int writes) throws IOException{
		
		Random r = new Random(System.currentTimeMillis());
		
		Vertex[] VSet = new Vertex[writes];
		
		for (int i = 0; i < writes; i++){
			Vertex v = graph.addVertex(null);
			v.setProperty("gid", i);
			VSet[i] = v;
		}
			
		
		for (int i = 0; i < writes; i++){
			Vertex v = VSet[i];
			for (int j = 0; j < Math.abs(r.nextInt()) % 20; j++){
				Vertex dstV = VSet[Math.abs(r.nextInt()) % writes];
				Edge e = graph.addEdge(null, v, dstV, "1");
				e.setProperty("start_ts", System.nanoTime());
				e.setProperty("end_ts", System.nanoTime());

				graph.commit();
			}
		}
		
	}
	
	public void close(){		 
		graph.shutdown();
	}
	public void run() throws IOException{
		this.connect();
		this.load(1000);
		this.close();
	}

	public String getTestName() {
		return "TitanTest";
	}
	
	public static void main(String[] args) throws IOException{
		TitanFullGraph t = new TitanFullGraph();
		t.run();
	}
}
