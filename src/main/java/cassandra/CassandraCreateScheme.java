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
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import utils.Constants;
import utils.PerformanceTest;
import workloads.GraphWorkLoad;
import workloads.MetaEdge;

public class CassandraCreateScheme{

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

    public CassandraCreateScheme(String server) {
        this.addr = server;
        this.connect();
    }

    public void close(){
        cluster.close();
    }

    public String getTestName() {
        return "CassandraTest";
    }

    public static void main(String[] args) throws IOException, InterruptedException{
        CassandraCreateScheme t = new CassandraCreateScheme(args[0]);
        t.createSchema();
    }
}