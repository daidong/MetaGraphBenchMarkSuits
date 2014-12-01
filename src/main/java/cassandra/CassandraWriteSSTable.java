package cassandra;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by daidong on 11/30/14.
 */
public class CassandraWriteSSTable {

    private int pid;

    public CassandraWriteSSTable(int pid){
        this.pid = pid;
    }

    public void run(int writes) throws IOException, InvalidRequestException {
        long start = System.currentTimeMillis();
        String keyspace = "/tmp/importPerf";
        String schema = "create table importPerf.mg ("
                + "gid int, "
                + "edgeType int,"
                + "dstid int,"
                + "edgeAttrs map<text, bigint>,"
                + "PRIMARY KEY (gid, edgeType, dstid));";

        String insert = "INSERT INTO importPerf.mg (gid, edgeType, dstid, edgeAttrs) VALUES (?, ?, ?, ?)";

        File directory = new File(keyspace);
        if (!directory.exists()) directory.mkdir();

        CQLSSTableWriter writer = CQLSSTableWriter.builder().inDirectory(directory).forTable(schema).using(insert).build();

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
                writer.addRow(srcV, edgeType + 1, dstV, edgeAttrs);

            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Insert Time: " + (end - start) + " ms");

        writer.close();
        end = System.currentTimeMillis();
        System.out.println("Insert Time: " + (end - start) + " ms");

    }
    public static void main(String[] args) throws IOException, InterruptedException, InvalidRequestException {
        int pid = Integer.parseInt(args[0]);
        CassandraWriteSSTable t = new CassandraWriteSSTable(pid);

        t.run(1000);

    }
}
