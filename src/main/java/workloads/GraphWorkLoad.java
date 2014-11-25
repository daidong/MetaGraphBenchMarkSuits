package workloads;

import java.util.ArrayList;

public interface GraphWorkLoad{

	public int getPressure();
	public int getVertex(int i);
	
	public MetaEdge getEdge(int i);
}
