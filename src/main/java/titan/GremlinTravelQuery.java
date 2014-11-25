package titan;

import java.io.IOException;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.gremlin.groovy.Gremlin;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.iterators.SingleIterator;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class GremlinTravelQuery {

	/*
	TitanGraph g = TitanFactory.build().set("storage.backend","cassandra").set("storage.hostname","192.168.1.66").open();
		Pipe pipe = Gremlin.compile("_().out('1').out('1').out('1')");
		pipe.setStarts(new SingleIterator<Vertex>(g.getVertex(7680256)));
		for(Object name : pipe) {
			  System.out.println((String) name);
		}
	 */
	public static final String INDEX_NAME = "search";
	public TitanManagement tmanagement;
	public TitanGraph graph;
			
	private static String CONFIG_PATH="/Users/daidong/Downloads/titan.properties";
	
	public void connect(){
		graph=TitanFactory.open(CONFIG_PATH);
		tmanagement = graph.getManagementSystem();
	}
	public void close(){		 
		graph.shutdown();
	}
	
	public void query(){
		for (TitanEdge te: graph.getVertex(7680256).getEdges()){
			TitanVertex tv = te.getOtherVertex(graph.getVertex(7680256));
			for (TitanEdge te1: tv.getEdges()){
				TitanVertex tv1 = te1.getOtherVertex(tv);
				for (TitanEdge te2: tv1.getEdges()){
					TitanVertex tv2 = te2.getOtherVertex(tv1);
					for (TitanEdge te3: tv2.getEdges()){
						TitanVertex tv3 = te3.getOtherVertex(tv2);
						for (TitanEdge te4: tv3.getEdges()){
							TitanVertex tv4 = te4.getOtherVertex(tv3);
						}
					}
				}
			}
		}
	}
	public void run() throws IOException{
		this.connect();
		long start = System.currentTimeMillis();
		this.query();
		System.out.println(System.currentTimeMillis() - start);
		this.close();
	}

	public String getTestName() {
		return "TitanTest";
	}
	
	public static void main(String[] args) throws IOException {
		GremlinTravelQuery q = new GremlinTravelQuery();
		q.run();
	}

}
