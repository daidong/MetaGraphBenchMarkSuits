package cassandra;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import utils.Constants;
import utils.PerformanceTest;
import workloads.GraphWorkLoad;
import workloads.MetaEdge;

public class CassandraPerformance implements PerformanceTest{
	
	private Cluster cluster;
	private Session session;
	
	private GraphWorkLoad gwl;
	
	public void connect(){
		InetSocketAddress server = new InetSocketAddress("localhost", 9042);
		ArrayList<InetSocketAddress> servers = new ArrayList<InetSocketAddress>();
		servers.add(server);
		
		cluster = Cluster.builder().addContactPointsWithPorts(servers).build();
		session = cluster.connect();
	}
	
	public static void createSchema(){
		InetSocketAddress server = new InetSocketAddress("localhost", 9042);
		ArrayList<InetSocketAddress> servers = new ArrayList<InetSocketAddress>();
		servers.add(server);
		
		Cluster cluster = Cluster.builder().addContactPointsWithPorts(servers)
		         .build();
		
		Session session = cluster.connect();
		//session.execute("CREATE KEYSPACE importPerf WITH replication " + 
		//	      "= {'class':'SimpleStrategy', 'replication_factor':1};");
		
		session.execute(
			      "create table importPerf.mg ("
			      + "gid varint, "
			      + "edgeType varint,"
			      + "dstid varint,"
			      + "edgeAttrs map<text, bigint>,"
			      + "PRIMARY KEY (gid, edgeType, dstid));");
		//+ "nodeAttrs map<text, text>,"
		System.out.println("Create Table Schema Return!");
		session.close();
		cluster.close();
	}
	
	public Session getSession(){
		return this.session;
	}
	
	public CassandraPerformance(GraphWorkLoad gwl) {
		this.gwl = gwl;
		this.connect();
	}
	
	public void close(){
		cluster.close();
	}
	
	public void load(GraphWorkLoad gwl) throws IOException{
		int stableVertexId = 0;
		for (int i = 0; i < gwl.getPressure(); i++){
			int vertexId = gwl.getVertex(i);
			MetaEdge edge = gwl.getEdge(i);
			if (vertexId != -1)
				stableVertexId = vertexId;
			else
				vertexId = stableVertexId;
			
			Map<String, Long> edgeAttrs = new HashMap<String, Long>();
			edgeAttrs.put("ts_start", edge.timstamps);
			edgeAttrs.put("ts_end", edge.timstamps);
			
			Statement state = QueryBuilder.insertInto("importPerf", "mg")
					.value("gid", vertexId)
					.value("edgeType", 1)
					.value("dstid", edge.edgeId)
					.value("edgeAttrs", edgeAttrs);
			//getSession().executeAsync(userState);
			getSession().execute(state);
		}
	}
	
	public void run() throws IOException{
		try{
			this.load(this.gwl);
		} finally {
			this.close();
		}
	}

	public String getTestName() {
		return "CassandraTest";
	}
}
