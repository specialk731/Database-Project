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
			//userCommand = userCommand.replaceAll("\\s+", " ");
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
		System.out.println("\tSHOW TABLES	----- SHOW TABLES INFO");
		System.out.println("\tCREATE TABLE	----- CREATE TABLE INFO");
		System.out.println("\tDROP TABLE	----- DROP TABLE INFO");
		System.out.println("\tINSERT INTO TABLE	- INSERT INTO TABLE INFO");
		System.out.println("\tUPDATE	--------- UPDATE INFO");
		System.out.println("\tSELECT FROM WHERE	- SELECT FROM WHERE INFO");
		System.out.println("\tHELP	------------- HELP INFO");
		System.out.println("\tEXIT	------------- EXIT INFO");

	}
	
	public static void parseUserCommand (String userCommand)
	{
		String[] commandTokens = userCommand.split(" ");
		
		switch (commandTokens[0])
		{
		case "show":
			if (commandTokens.length > 1 && commandTokens[1].equals("tables"))
			{
				SHOW_TABLES();
			}
			else
				System.out.println("Got UKNOWN command: " + userCommand);
			break;
			
		case "create":
			if (commandTokens.length > 2 && commandTokens[1].equals("table"))
			{
				CREATE_TABLE(userCommand);
			}
			else
				System.out.println("Got UKNOWN command: " + userCommand);
			break;
			
		case "drop":
			if (commandTokens.length > 2 && commandTokens[1].equals("table"))
			{
				DROP_TABLE(userCommand);
			}
			else
				System.out.println("Got UKNOWN command: " + userCommand);
			break;
			
		case "insert":
			if (commandTokens.length == 6 && commandTokens[1].equals("into") && commandTokens[2].equals("table") && commandTokens[4].equals("values"))
			{
				INSERT_INTO_TABLE(userCommand);
			}
			else
				System.out.println("Got UKNOWN command: " + userCommand);
			break;
			
		case "update":
			if (commandTokens[2].equals("where"))
					UPDATE(userCommand);
			break;
			
		case "select":
			SELECT_FROM_WHERE(userCommand);	
			break;
			
		case "help":
			help();
			break;
			
		case "exit":
			break;
		
		default:
			System.out.println("Got UKNOWN or INCOMPLETE command: " + userCommand);
			break;
		}
	}
	
	public static void SHOW_TABLES()
	{
		System.out.println("GOT COMMAND SHOW TABLES");
		SELECT_FROM_WHERE();
	}
	
	public static void DROP_TABLE(String usrCom)
	{
		String[] dropped_table = usrCom.split(" ");
		
		dropped_table[2] = dropped_table[2].replace(";", "");
		
		System.out.println("Dropped table: " + dropped_table[2]);
	}
	
	public static void CREATE_TABLE(String usrCom)
	{
		String[] columns = usrCom.split(",");
		
		String[] tmp = columns[0].split("\\(", 2);
		
		columns[0]=tmp[1];
		
		columns[columns.length - 1] = columns[columns.length-1].replace(")", "");
		
		for(int i = 0; i < columns.length; i++)
			System.out.println(columns[i]);
		
		
		
		/*//Vector<String> cols = new Vector<String>(comTok.length/2, 2);
		
		String[] columns = null;
		String[] tmp = new String[2];
		
		columns = new String[comTok.length];
		
		for(int k = 0; k < columns.length; k++)
			columns[k] = "";
		
		System.out.println("Got command CREATE TABLE " + comTok[2]);
			
		int j = 0;
		
		for(int i = 3; i < comTok.length; i++)
			{
			if(comTok[i].contains(","))
			{
				tmp = comTok[i].split(",");
				columns[j] = columns[j] + " " + tmp[0];
				j++;
				columns[j] = columns[j] + tmp[1];
			}
			else
			{
				if (columns[j].equals(""))
					columns[j] = comTok[i];
				else
					columns[j] = columns[j] + " " + comTok[i];
			}
			}
		
		columns[0] = columns[0].replace("(", "");
		columns[j] = columns[j].replace(")","");
		
		System.out.println("col length = " + columns.length);
		//System.out.println("vector length = " + cols.size());
		System.out.println("Number of Columns = " + (j+1));
		
		j = 0;
		
		while(!columns[j].equals("") && j < columns.length)
		{
			System.out.println(j + ") " + columns[j]);
			j++;
		}*/
	}
	
	public static void INSERT_INTO_TABLE(String usrCom)
	{
		String[] values = usrCom.split(",");
		
		String[] tmp = values[0].split("\\(", 2);
		
		values[0]=tmp[1];
		
		values[values.length - 1] = values[values.length-1].replace(")", "");
		
		for(int i = 0; i < values.length; i++)
			System.out.println(values[i]);		
		
		
		/*String[] values = null;
		
		System.out.println("Got command INSERT INTO TABLE " + comTok[3]);
		System.out.println("Num Tokens: " + comTok.length);
		System.out.println("The values: " + comTok[5]);
		
		values = comTok[5].split(",");
		
		values[0] = values[0].replace("(", "");
		values[values.length - 1] = values[values.length - 1]. replace(")","");
		
		for(int i = 0; i < values.length; i++)
			System.out.println(values[i]);*/
	}
	
	public static void UPDATE(String usrCom)
	{
		
	}
	
	public static void SELECT_FROM_WHERE(String usrCom)
	{
		
	}
	
}




