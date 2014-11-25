package workloads;

import java.util.ArrayList;
import java.util.Random;

/**
 * This class generates workloads (n writes) on one vertex
 * 
 * @author daidong
 *
 */
public class ConcurrentWorkLoad implements GraphWorkLoad {
	
	public int writes;
	public int vertexId;
	public ArrayList<MetaEdge> edges;
	
	public ConcurrentWorkLoad(int currentWrites){
		this.writes = currentWrites;
		this.edges = new ArrayList<MetaEdge>();
		
		Random r = new Random(System.currentTimeMillis());
		this.vertexId = 0;
		
		for (int i = 0; i < this.writes; i++){
			MetaEdge edge = new MetaEdge(Math.abs(r.nextInt()));
			edges.add(edge);
		}
	}
	
	public int getVertex(int i){
		if (i == 0)
			return this.vertexId;
		else
			return -1;
	}
	
	public MetaEdge getEdge(int i){
		return edges.get(i); 
	}

	public int getPressure() {
		return this.writes;
	}
	
	
}
