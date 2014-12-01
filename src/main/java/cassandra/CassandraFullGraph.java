package cassandra;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.thinkaurelius.titan.util.system.Threads;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import utils.Constants;
import utils.PerformanceTest;
import workloads.GraphWorkLoad;
import workloads.MetaEdge;

public class CassandraFullGraph{
	
	private Cluster cluster;
	private Session session;
	private String addr;
    private int pid;


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

    public CassandraFullGraph(String server, int pid) {
        this.addr = server;
        this.pid = pid;
        this.connect();
    }
	
	public void close(){
		cluster.close();
	}
	
	public void load(int writes) throws IOException{

        Random r = new Random(System.currentTimeMillis());

        int[] vset = new int[writes];
        for (int i = 0; i < writes; i++){
            vset[i] = this.pid * writes + i;
        }

        Map<String, Long> edgeAttrs = new HashMap<String, Long>();
        edgeAttrs.put("ts_start", System.nanoTime());
        edgeAttrs.put("ts_end", System.nanoTime());

        for (int i = 0; i < writes; i++){
            int srcV = vset[i];
            for (int j = 0; j < Math.abs(r.nextInt()) % 20; j++){

                int dstV = vset[Math.abs(r.nextInt()) % writes];

                int edgeType = Math.abs(r.nextInt()) % 20;
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

    public void asyncload(int writes) throws IOException{

        Random r = new Random(System.currentTimeMillis());

        int[] vset = new int[writes];
        for (int i = 0; i < writes; i++){
            vset[i] = this.pid * writes + i;
        }

        Map<String, Long> edgeAttrs = new HashMap<String, Long>();
        edgeAttrs.put("ts_start", System.nanoTime());
        edgeAttrs.put("ts_end", System.nanoTime());

        List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();
        for (int i = 0; i < writes; i++){
            int srcV = vset[i];
            for (int j = 0; j < Math.abs(r.nextInt()) % 20; j++){

                int dstV = vset[Math.abs(r.nextInt()) % writes];

                int edgeType = Math.abs(r.nextInt()) % 20;
                Statement state = QueryBuilder.insertInto("importPerf", "mg")
                        .value("gid", srcV)
                        .value("edgeType", edgeType+1)
                        .value("dstid", dstV)
                        .value("edgeAttrs", edgeAttrs);
                ResultSetFuture p = getSession().executeAsync(state);
                futures.add(p);
                //getSession().execute(state);
            }
        }

        for (ResultSetFuture f : futures){
            f.getUninterruptibly();
        }

    }

    public void batchload(int writes) throws IOException{

        Random r = new Random(System.currentTimeMillis());

        int[] vset = new int[writes];
        for (int i = 0; i < writes; i++){
            vset[i] = this.pid * writes + i;
        }

        Map<String, Long> edgeAttrs = new HashMap<String, Long>();
        edgeAttrs.put("ts_start", System.nanoTime());
        edgeAttrs.put("ts_end", System.nanoTime());

        //final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        final BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        for (int i = 0; i < writes; i++){
            int srcV = vset[i];
            for (int j = 0; j < Math.abs(r.nextInt()) % 20; j++){

                int dstV = vset[Math.abs(r.nextInt()) % writes];

                int edgeType = Math.abs(r.nextInt()) % 20;
                Statement state = QueryBuilder.insertInto("importPerf", "mg")
                        .value("gid", srcV)
                        .value("edgeType", edgeType+1)
                        .value("dstid", dstV)
                        .value("edgeAttrs", edgeAttrs);
                batch.add(state);
                //getSession().execute(state);
            }
        }
        getSession().execute(batch);
    }

    public void run(int type) throws IOException, InvalidRequestException {
		try{
            if (type == 1)
			    this.load(1000);
            else if (type == 2)
                this.asyncload(1000);
            else if (type == 3)
                this.batchload(1000);
		} finally {
			this.close();
		}
	}

	public String getTestName() {
		return "CassandraTest";
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, InvalidRequestException {
        int pid = Integer.parseInt(args[1]);
        int insertType = Integer.parseInt(args[2]);
        CassandraFullGraph t = new CassandraFullGraph(args[0], pid);
	    /*
	    try{
		t.createSchema();
	    } catch (AlreadyExistsException aee){
		System.out.println("schema exists");
	    }
	    */
        long start = System.currentTimeMillis();
        t.run(insertType);
        long end = System.currentTimeMillis();
        System.out.println("Insert Time: " + (end - start) + " ms from " + args[0] + " pid");
		
	}
}