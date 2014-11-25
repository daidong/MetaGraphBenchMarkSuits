package workloads;

import java.util.Random;

public class MetaEdge {
	public long timstamps;
	public int edgeId;
	public int type;
	
	public MetaEdge(int id, int type){
		this.timstamps = System.currentTimeMillis();
		this.type = type;
		this.edgeId = id;
	}
	
	public MetaEdge(int id){
		this.timstamps = System.currentTimeMillis();
		Random r = new Random(System.currentTimeMillis());
		this.type = Math.abs(r.nextInt());
		this.edgeId = id;
	}
}
