package workloads;

import java.util.ArrayList;
import java.util.Random;

public class RandomWorkLoad implements GraphWorkLoad {
	
	public int writes;
	public int vertexId;
	public ArrayList<MetaEdge> edges;
	
	public RandomWorkLoad(int currentWrites){
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
		Random r = new Random(System.currentTimeMillis());
		return Math.abs(r.nextInt());
	}
	
	public MetaEdge getEdge(int i){
		return edges.get(i); 
	}

	public int getPressure() {
		return this.writes;
	}
	
	
}
