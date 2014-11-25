package utils;

import java.io.IOException;

public interface PerformanceTest {
	public void run() throws IOException;
	public String getTestName();
}
