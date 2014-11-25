package utils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;


public class SplitFile {

	public void split(String path, int number) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line;
		
		ArrayList<BufferedWriter> wrs = new ArrayList<BufferedWriter>();
		for (int i = 0; i < number; i++){
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(path+"."+i))));
			wrs.add(bw);
		}
		
		int rr = 0;
		
		while ((line = br.readLine()) != null) {
			BufferedWriter bw = wrs.get(rr);
			bw.write(line);
			bw.newLine();
			rr = (rr+1) % number;
		}
		
		br.close();
		for (int i = 0; i < number; i++){
			wrs.get(i).close();
		}
	}
	public static void main(String[] args) throws IOException {
		SplitFile sf = new SplitFile();
		sf.split("/Users/daidong/Documents/darshan-trace/log-2013-1-9.txt", 6);
	}

}
