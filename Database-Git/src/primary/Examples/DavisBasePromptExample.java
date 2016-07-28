package primary.Examples;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;



/**
 * @author Chris Irwin Davis
 * @version 1.0
 * <b>This is an example of how to read/write binary data files using RandomAccessFile class</b>
 *
 */
public class DavisBasePromptExample {

	/* This can be changed to whatever you like */
	static String prompt = "davisql> ";

	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	
    public static void main(String[] args) {

		/* Display the welcome screen */
		splashScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = ""; 

		while(!userCommand.equals("exit")) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");


	}

	///////////////////////////////////////////////////////////////////////////////////////////////
	//
	//  Method definitions
	//

	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		// version();
		System.out.println("Type \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
		/**
		 *  Help: Display supported commands
		 */
		public static void help() {
			System.out.println(line("*",80));
			System.out.println("SUPPORTED COMMANDS");
			System.out.println("All commands below are case insensitive");
			System.out.println();
			System.out.println("\tSELECT * FROM table_name;                        Display all records in the table.");
			System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  Display records whose rowid is <id>.");
			System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
			System.out.println("\tVERSION;                                         Show the program version.");
			System.out.println("\tHELP;                                            Show this help information");
			System.out.println("\tEXIT;                                            Exit the program");
			System.out.println();
			System.out.println();
			System.out.println(line("*",80));
		}

	/** Display the DavisBase version */
	public static void version() {
		System.out.println("DavisBaseLite v1.0\n");
	}
	
	
	public static void parseUserCommand (String userCommand) {
		/*
		*  This switch handles a very small list of hardcoded commands of known syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement */
		String[] commandTokens = userCommand.split(" ");
		
		
		switch (commandTokens[0]) {
			case "select":
				System.out.println("DEBUG: Call your method to process queries");
				break;
			case "drop":
				System.out.println("DEBUG: Call your method to remove items");
				break;
			case "help":
				help();
				break;
			case "version":
				version();
				break;
			case "exit":
				break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}
	
}