package workloads;

import java.util.ArrayList;
import java.util.Random;

public class FullWorkLoad implements GraphWorkLoad {
	
	public int writes;
	public ArrayList<Edge> edges;
	public ArrayList<Integer> vertices;
	
	public FullWorkLoad(int currentWrites){
		this.writes = currentWrites;
		this.edges = new ArrayList<Edge>();
		this.vertices = new ArrayList<Integer>();
		Random r = new Random(System.currentTimeMillis());
				
		int[] vset = new int[currentWrites];
		for (int i = 0; i < currentWrites; i++){
			vset[i] = Math.abs(r.nextInt());
		}
		
		int v = Math.abs(r.nextInt()) % currentWrites;
		vertices.add(v);
		for (int i = 0; i < this.writes; i++){
			int dstV = Math.abs(r.nextInt()) % currentWrites;
			Edge edge = new Edge(vset[v], vset[dstV]);
			edges.add(edge);
		}
	}
	
	public int getVertex(int i){
		return vertices.get(i);
	}
	
	public Edge edge(int i){
		return edges.get(i); 
	}
	
	public MetaEdge getEdge(int i){
		return null; 
	}

	public int getPressure() {
		return this.writes;
	}
	
	
}
