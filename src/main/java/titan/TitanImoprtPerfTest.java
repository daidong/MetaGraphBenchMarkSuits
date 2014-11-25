package titan;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import utils.PerformanceTest;
import workloads.GraphWorkLoad;
import workloads.MetaEdge;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;


public class TitanImoprtPerfTest implements PerformanceTest {

	public static final String INDEX_NAME = "search";
	public TitanManagement tmanagement;
	public TitanGraph graph;
		
	private GraphWorkLoad gwl;
	
	private static String CONFIG_PATH="/Users/daidong/Downloads/titan.properties";
	
	public TitanImoprtPerfTest(GraphWorkLoad gwl) {
		this.gwl = gwl;
	}
	
	public void connect(){
		graph=TitanFactory.open(CONFIG_PATH);
		tmanagement = graph.getManagementSystem();
	}
		
	public void createSchema(){
		
	}
	
	public void load(GraphWorkLoad gwl) throws IOException{
		Vertex stableV = null;
		for (int i = 0; i < gwl.getPressure(); i++){
			int vertexId = gwl.getVertex(i);
			MetaEdge edge = gwl.getEdge(i);
			Vertex v = null, v1 = null;
			Edge e;
			if (vertexId != -1){
				v = graph.addVertex(null);
				stableV = v;
				v.setProperty("uid", vertexId);
			}
			else
				v = graph.getVertex(stableV.getId());
			
			if (edge != null){
				v1 = graph.addVertex(null);
				e = graph.addEdge(edge.edgeId, v, v1, "1");
				e.setProperty("start_ts", edge.timstamps);
				e.setProperty("end_ts", edge.timstamps);
			}
			graph.commit();
				
		}
		
	}
	
	public void close(){		 
		graph.shutdown();
	}
	public void run() throws IOException{
		this.connect();
		this.createSchema();
		this.load(this.gwl);
		this.close();
	}

	public String getTestName() {
		return "TitanTest";
	}
}
