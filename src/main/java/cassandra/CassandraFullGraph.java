package cassandra;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.thinkaurelius.titan.util.system.Threads;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import utils.Constants;
import utils.PerformanceTest;
import workloads.GraphWorkLoad;
import workloads.MetaEdge;

public class CassandraFullGraph implements PerformanceTest{
	
	private Cluster cluster;
	private Session session;
	private String addr;
	
	public void connect(){
		InetSocketAddress server = new InetSocketAddress(this.addr, 9042);
		ArrayList<InetSocketAddress> servers = new ArrayList<InetSocketAddress>();
		servers.add(server);
		
		cluster = Cluster.builder().addContactPointsWithPorts(servers).build();
		session = cluster.connect();
	}
	
	public void createSchema(){
		InetSocketAddress server = new InetSocketAddress(this.addr, 9042);
		ArrayList<InetSocketAddress> servers = new ArrayList<InetSocketAddress>();
		servers.add(server);
		
		Cluster cluster = Cluster.builder().addContactPointsWithPorts(servers)
		         .build();
		
		Session session = cluster.connect();
		session.execute("CREATE KEYSPACE importPerf WITH replication " + 
			      "= {'class':'SimpleStrategy', 'replication_factor':1};");
		
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
	
	public CassandraFullGraph(String server) {
		this.addr = server;
		this.connect();
	}
	
	public void close(){
		cluster.close();
	}
	
	public void load(int writes) throws IOException{
		
		Random r = new Random(System.currentTimeMillis());
					
		int[] vset = new int[writes];
		for (int i = 0; i < writes; i++){
			vset[i] = i;
		}
			
		for (int i = 0; i < writes; i++){
			int srcV = vset[i];
			for (int j = 0; j < Math.abs(r.nextInt()) % 20; j++){
				
				int dstV = vset[Math.abs(r.nextInt()) % writes];

				Map<String, Long> edgeAttrs = new HashMap<String, Long>();
				edgeAttrs.put("ts_start", System.nanoTime());
				edgeAttrs.put("ts_end", System.nanoTime());

				int edgeType = Math.abs(r.nextInt()) % 2;
				Statement state = QueryBuilder.insertInto("importPerf", "mg")
						.value("gid", srcV)
						.value("edgeType", edgeType+1)
						.value("dstid", dstV)
						.value("edgeAttrs", edgeAttrs);
				//getSession().executeAsync(userState);
				getSession().execute(state);
			}
		}
	}
	
	
	
	public void run() throws IOException{
		try{
			this.load(1000);
		} finally {
			this.close();
		}
	}

	public String getTestName() {
		return "CassandraTest";
	}
	
	public static void main(String[] args) throws IOException, InterruptedException{
		int id = Integer.parseInt(args[1]);
		CassandraFullGraph t = new CassandraFullGraph("192.168.1.67");
		try{
			t.createSchema();
		} catch (AlreadyExistsException aee){
			System.out.println("schema exists");
		}
		long start = System.currentTimeMillis();
		t.run();
		
	}
}
