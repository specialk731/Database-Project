package primary;

import java.io.*;
import java.util.*;

public class GreenwaldBase {
	
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	static String prompt = "Greenwaldsql> ";

	public static void main(String[] args) {
		introScreen();
		
		String userCommand = "";
		
		while(!userCommand.equals("exit"))
		{
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
	}

	
	public static void introScreen()
	{
		System.out.println(line("-",80));
		System.out.println("GreenwaldBase:");
		System.out.println("Type \"help;\" to display commands");
		System.out.println(line("-",80));
	}
	
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	public static void help()
	{
		System.out.println("Allowed Commands:");
		System.out.println("\tSELECT -- SELECT INFO");
		System.out.println("\tDROP ---- DROP INFO");
		System.out.println("\tHELP ---- HELP INFO");
		System.out.println("\tVERSION - VERSION INFO");
		System.out.println("\tEXIT ---- EXIT INFO");

	}
	
	public static void parseUserCommand (String userCommand)
	{
		String[] commandTokens = userCommand.split(" ");
		
		switch (commandTokens[0])
		{
		case "select":
			System.out.println("Got command SELECT");
			break;
		case "drop":
			System.out.println("Got command DROP");
			break;
		case "help":
			help();
			break;
		case "version":
			System.out.println("Got command VERSION");
			break;
		case "exit":
			break;
		
		default:
			System.out.println("Got UKNOWN command: " + userCommand);
			break;
		}
	}
}