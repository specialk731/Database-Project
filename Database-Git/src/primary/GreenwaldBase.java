package primary;

import java.io.*;

public class GreenwaldBase {

	public static void main(String[] args) {
		
		try{
		PrintWriter writer = new PrintWriter("data/Table_1.tbl", "UTF-8");
		writer.println("The first line");
		writer.println("The second line");
		writer.close();
		
		PrintWriter writer2 = new PrintWriter("data/Index_1.ndx", "UTF-8");
		writer2.println("The first line");
		writer2.println("The second line");
		writer2.close();
		
		System.out.println("Finished");
	
		}
		catch (Exception e)
		{
			System.out.println("Exception");
		}
	}

}