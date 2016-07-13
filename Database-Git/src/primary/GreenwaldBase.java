package primary;

import java.io.*;

public class GreenwaldBase {

	public static void main(String[] args) {
		
		try{
		PrintWriter writer = new PrintWriter("data/Test.txt", "UTF-8");
		writer.println("The first line");
		writer.println("The second line");
		writer.close();
		
		System.out.println("Finished");
	
		}
		catch (Exception e)
		{
			System.out.println("Exception");
		}
	}

}