package group.project;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilterReader;
import java.io.InputStream;
import java.io.InputStreamReader;


public class centroid {

	/**
	 * @param args
	 */
	

	public void display() {
		// TODO Auto-generated method stub
		try {
			FileReader fp = new FileReader("result.txt");
			BufferedReader br = new BufferedReader(fp);
			for (int c = 0; c < 10; c++) {
				System.out.println(br.readLine());
			}
			br.close();
			fp.close();
		} catch (Exception e) {

		}
	}

}
