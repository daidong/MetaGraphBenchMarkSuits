package OrientDB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import utils.PerformanceTest;
import workloads.GraphWorkLoad;
import workloads.MetaEdge;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;


public class OrientDBImportPerfTest implements PerformanceTest {

	private OrientGraph graph;
	
	private GraphWorkLoad gwl;
	
	
	public void connect(String... node){
		graph = new OrientGraph("remote:localhost/testg");
		//graph = new OrientGraph("plocal:/tmp/tmpdb");
	}
	
	public void createSchema(){
	}
	
	public OrientDBImportPerfTest(){}
	public OrientDBImportPerfTest(GraphWorkLoad gwl) {
		this.gwl = gwl;
	}
	
	public void close(){
		graph.shutdown();
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
				v1.setProperty("jid", edge.edgeId);
				e = graph.addEdge(edge.edgeId, v, v1, "1");
				e.setProperty("start_ts", edge.timstamps);
				e.setProperty("end_ts", edge.timstamps);
			}
			graph.commit();
				
		}
	}
	
	public void run() throws IOException{
		this.connect();
		this.createSchema();
		try{
			this.load(this.gwl);
		} finally {
			this.close();
		}
	}
	
	public static void main(String[] args) throws IOException{
		OrientDBImportPerfTest db = new OrientDBImportPerfTest();
		db.run();
	}

	public String getTestName() {
		return "OrientDBTest";
	}
}

/*
 * <dependency>
  		<groupId>com.tinkerpop</groupId>
  		<artifactId>pipes</artifactId>
  		<version>2.5.0</version>
  	</dependency>
  	<dependency>
  		<groupId>com.tinkerpop.blueprints</groupId>
  		<artifactId>blueprints-core</artifactId>
  		<version>2.5.0</version>
  	</dependency>
  	<dependency>
  		<groupId>com.tinkerpop.gremlin</groupId>
  		<artifactId>gremlin-java</artifactId>
  		<version>2.5.0</version>
  	</dependency>
  	<dependency>
  		<groupId>com.tinkerpop.gremlin</groupId>
  		<artifactId>gremlin-groovy</artifactId>
  		<version>2.5.0</version>
  	</dependency>
*/