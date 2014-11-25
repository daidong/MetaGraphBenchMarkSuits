package tests;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class TestClass {

	public static void main(String[] args) {
		Date t = new Date(1357029012000L);
		System.out.println(t);
		
		List<List<String>> edgesWhere = new ArrayList<List<String>>();
		
		List<String> n = new ArrayList<String>();
		
		edgesWhere.add(n);
		
		System.out.println(edgesWhere.size());
	}

}
