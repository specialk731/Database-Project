package primary;

import java.io.*;
import java.util.*;

public class GreenwaldBase {
	
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	static String prompt = "Greenwaldsql> ";
	static long pageSize = 512; 
	static long tables_rowid = 3, columns_rowid = 9;

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
		try{
			
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
			if (commandTokens[1].equals("into") && commandTokens[2].equals("table") && commandTokens[4].equals("values"))
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
		catch(Exception e)
		{
			System.out.println("Got UKNOWN or INCOMPLETE command: " + userCommand);
		}
	}
	
	public static void SHOW_TABLES()
	{
		System.out.println("GOT COMMAND SHOW TABLES");
		SELECT_FROM_WHERE("select * from greenwaldbase_tables;");
	}
	
	public static void DROP_TABLE(String usrCom)
	{
		String[] dropped_table = usrCom.split(" ");
		
		dropped_table[2] = dropped_table[2].replace(";", "");
		
		System.out.println("Dropped table: " + dropped_table[2]);
		
		File file = new File(dropped_table[2] + ".tbl");
		
		if(file.isFile())
		{
			file.delete();
			
			DELETE_FROM_WHERE("DELETE FROM TABLE greenwaldbase_tables WHERE table_name = \"" + dropped_table[2] + "\";"); //Need to delete it from the meta-data
			
		}
		else
		{
			System.out.println("Error: Could not find table \"" + dropped_table[2] + "\"");
		}
	}
	
	public static void CREATE_TABLE(String usrCom)
	{		
		String table = usrCom.substring(13, usrCom.indexOf(" ", 13));
		
		File f = new File(table + ".tbl");
		
		if(!f.isFile())
		{		
			String[] columns = usrCom.toLowerCase().split(",");
			
			String[] tmp = columns[0].split("\\(", 2);
					
			if(usrCom.toLowerCase().contains("primary key"))
				usrCom = usrCom.toLowerCase().replace("primary key", "");
					
			columns[0]=tmp[1];
			
			columns[columns.length - 1] = columns[columns.length-1].replace(")", "");
			
			for(int i = 0; i < columns.length; i++)
				System.out.println(columns[i] = columns[i].trim());
			
			System.out.println("Table name: " + table);
			
			RandomAccessFile binaryFile;
			
			try {
				binaryFile = new RandomAccessFile("data\\" + table + ".tbl", "rw");
				
				binaryFile.setLength(0);
				binaryFile.setLength(pageSize);
				binaryFile.seek(0);
				binaryFile.writeByte(0x0D);
				
				binaryFile.close();
				
				File file = new File("data\\greenwaldbase_tables.tbl");
				
				if(!file.isFile())			
					Create_greenwaldbase_tables();
	
				INSERT_INTO_TABLE("INSERT INTO TABLE greenwaldbase_tables VALUES (" + tables_rowid++ + "," + table +",0);"); // INSERT THE TABLE INTO greenwaldbase_tables
				
				file = new File("data\\greenwaldbase_columns.tbl");
				
				if(!file.isFile())
					Create_greenwaldbase_columns();
				
				int pos;
				String col_name, data_type;
				
				for(int i = 0; i < columns.length; i++)
				{		
					columns[i] = columns[i].trim();
					col_name = columns[i].substring(0, columns[i].indexOf(" "));
					pos = i + 1;
							
					if(columns[i].contains("[not null]"))
					{
						data_type = (columns[i].substring(columns[i].indexOf(" ") + 1		, 	columns[i].indexOf(" ", columns[i].indexOf(" ") + 1)).toUpperCase());
						
						INSERT_INTO_TABLE("INSERT INTO TABLE greenwaldbase_columns VALUES (" + columns_rowid++ + ", " + table + ", " + col_name + ", " + data_type + ", " + pos + ", NO);");
	
					}
					else
					{
						data_type =  columns[i].substring(columns[i].indexOf(" ") + 1).toUpperCase();
					
						INSERT_INTO_TABLE("INSERT INTO TABLE greenwaldbase_columns VALUES (" + columns_rowid++ + ", " + table + ", " + col_name + ", " + data_type + ", " + pos + ", YES);");
					}
				}
	
	
			}
			catch(Exception e1)
			{
				System.out.println(e1);
			}
			
		}
		else
			System.out.println("The table " + table + " already exists");
	}
	
	public static void INSERT_INTO_TABLE(String usrCom) //Values can't have strings with , in them... at all...
	{
		String table = usrCom.substring(18, usrCom.indexOf(" ", 18));
		
		File file = new File("data\\" + table + ".tbl");
		
		if(file.isFile())
		{
		String[] values = usrCom.split(",");
		
		String[] tmp = values[0].split("\\(", 2);
		
		values[0]=tmp[1];
		
		values[values.length - 1] = values[values.length-1].replace(");", "");
		
		for(int i = 0; i < values.length; i++)
			values[i] = values[i].trim();
						
		int key = Integer.parseInt(values[0]);
		
		String type = SELECT_FROM_WHERE_tostring("SELECT DATA_TYPE FROM greenwaldbase_tables WHERE table_name=\"" + table + "\";");
		
		short payload = 0;
		
		for(int i = 0; i < values.length ;i++)
		{

		}
				
		try{
			
		long pointer = 0, tmpPointer = 0, pagePointer = 0;
		int tempKey = 0;
			
		RandomAccessFile binaryFile = new RandomAccessFile("data\\" + table + ".tbl", "rw");
		
		binaryFile.seek(0);
		
		if(binaryFile.readByte() == 0x0d && binaryFile.readByte() == 0x00) //There is nothing inserted into the table at all
		{
			binaryFile.seek(0);						//Start at the beginning
			binaryFile.readByte(); 					//Skip the info about page type
			binaryFile.writeByte(1);				//Write that there is now 1 record on this page
			pointer = pageSize - (payload + 6);		//Point to where the record will begin
			binaryFile.writeShort((short)pointer);	//Write the address of the record start
			binaryFile.seek(pointer);				//File looks at pointer
			binaryFile.writeShort(payload);			//Write Size of Payload
			binaryFile.writeInt(key);				//Write the Key
			binaryFile.writeByte(values.length - 1);//Write Num cols not including key
			
			binaryFile.close();
		}
		else
		{		
			binaryFile.seek(0);
			
			if(binaryFile.readByte() == 0x05)
			{
				//Find the leaf it goes in
				//Make pagePointer the address of the page start
			}
					
			byte numRecords = binaryFile.readByte();
			
			
			
			
			
			binaryFile.close();
			
		}
		
		}
		catch(Exception e2)
		{
			System.out.println(e2);
		}
		}
		else
			System.out.println("The table: \"" + table + "\" could not be found.");
		
	}
	
	public static void UPDATE(String usrCom)
	{
		
	}
	
	public static void SELECT_FROM_WHERE(String usrCom)
	{
		System.out.println("In function SELECT FROM WHERE " + usrCom);
		
		
	}
	
	public static String SELECT_FROM_WHERE_tostring(String usrCom)
	{
		return "";
	}
	
	public static void DELETE_FROM_WHERE(String usrCom)
	{
		
	}
	
	public static void Create_greenwaldbase_tables()
	{		
		try{
		RandomAccessFile bf = new RandomAccessFile("data\\greenwaldbase_tables.tbl", "rw");
		bf.setLength(0);
		bf.setLength(pageSize);
		bf.seek(0);
		bf.writeByte(0x0d);		//Leaf Node
		bf.writeByte(0x02);		//2 Cells
		bf.writeShort((short)(pageSize - (30 + 31 + 1)));		//Offset of start of content area
		bf.writeShort((short)(pageSize - (30 + 31 + 1)));		//Offset of smallest (rowid = 1);
		bf.writeShort((short)(pageSize - (31 + 1)));		//Offset of largest (rowid = 2);
		
		bf.seek(pageSize - (30 + 31 + 1));						//Locate the place to start writting
		bf.writeShort(24);					//payload of row id 1
		bf.writeInt(1);						//Key for 1
		bf.writeByte(0x01);					//1 col in addition to key
		bf.writeByte(0x0C + 20);			//Data type is a string with 16 characters
		bf.writeUTF("greenwaldbase_tables");//The stored string
		
		bf.writeShort(25);
		bf.writeInt(2);
		bf.writeByte(0x01);
		bf.writeByte(0x0C + 21);
		bf.writeUTF("greenwaldbase_columns");//The stored string
		
		bf.close();
		}
		catch(Exception e5)
		{
			System.out.println(e5);
		}
	}
	
	public static void Create_greenwaldbase_columns()
	{
		try{
			RandomAccessFile bf = new RandomAccessFile("data\\greenwaldbase_columns.tbl", "rw");
			
			bf.setLength(0);
			bf.setLength(pageSize);
			
			bf.seek(0);
			bf.writeByte(0x0d);		//Leaf Node
			bf.writeByte(0x08);		//8 Cells
			bf.writeShort((short)(pageSize - (51 + 57 + 52 + 58 + 59 + 57 + 67 + 59 + 1)));	//Offset of start of content area
			bf.writeShort((short)(pageSize - (51 + 57 + 52 + 58 + 59 + 57 + 67 + 59 + 1)));	//Offset of smallest (rowid = 1);
			bf.writeShort((short)(pageSize - (57 + 52 + 58 + 59 + 57 + 67 + 59 + 1)));		//Offset of (rowid = 2);
			bf.writeShort((short)(pageSize - (52 + 58 + 59 + 57 + 67 + 59 + 1)));			//Offset of (rowid = 3);
			bf.writeShort((short)(pageSize - (58 + 59 + 57 + 67 + 59 + 1)));				//Offset of (rowid = 4);
			bf.writeShort((short)(pageSize - (59 + 57 + 67 + 59 + 1)));						//Offset of (rowid = 5);
			bf.writeShort((short)(pageSize - (57 + 67 + 59 + 1)));							//Offset of (rowid = 6);
			bf.writeShort((short)(pageSize - (67 + 59 + 1)));								//Offset of (rowid = 7);
			bf.writeShort((short)(pageSize - (59 + 1)));										//Offset of largest (rowid = 7);
			
			bf.seek(pageSize - (51 + 57 + 52 + 58 + 59 + 57 + 67 + 59 + 1));				//Locate the place to start writing
			
			//Cell 1		
			bf.writeShort(45);					//payload of row id 1
			bf.writeInt(1);						//Key for 1
			bf.writeByte(0x05);					//5 col in addition to key
			bf.writeByte(0x0C + 20);			//col2 data type
			bf.writeByte(0x0C + 5);				//col3 data type
			bf.writeByte(0x0C + 3);				//col4 data type
			bf.writeByte(0x04);					//col5 data type
			bf.writeByte(0x0C + 2);				//col6
			
			bf.writeUTF("greenwaldbase_tables");//col2 table name
			bf.writeUTF("rowid");				//col3 column name
			bf.writeUTF("INT");					//col4 data type
			bf.writeByte(1);					//col5 ordinal position
			bf.writeUTF("NO");					//col6 is nullable
			
			//Cell 2
			bf.writeShort(51);					//payload of row id 1
			bf.writeInt(2);						//Key for 1
			bf.writeByte(0x05);					//5 col in addition to key
			bf.writeByte(0x0C + 20);			//col2 data type
			bf.writeByte(0x0C + 10);			//col3 data type
			bf.writeByte(0x0C + 4);				//col4 data type
			bf.writeByte(0x04);					//col5 data type
			bf.writeByte(0x0C + 2);				//col6
			
			bf.writeUTF("greenwaldbase_tables");//col2 table name
			bf.writeUTF("table_name");			//col3 column name
			bf.writeUTF("TEXT");				//col4 data type
			bf.writeByte(2);					//col5 ordinal position
			bf.writeUTF("NO");					//col6 is nullable
			
			//Cell 3
			bf.writeShort(46);					//payload of row id 1
			bf.writeInt(3);						//Key for 1
			bf.writeByte(0x05);					//5 col in addition to key
			bf.writeByte(0x0C + 21);			//col2 data type
			bf.writeByte(0x0C + 5);				//col3 data type
			bf.writeByte(0x0C + 3);				//col4 data type
			bf.writeByte(0x04);					//col5 data type
			bf.writeByte(0x0C + 2);				//col6
			
			bf.writeUTF("greenwaldbase_columns");//col2 table name
			bf.writeUTF("rowid");				//col3 column name
			bf.writeUTF("INT");					//col4 data type
			bf.writeByte(1);					//col5 ordinal position
			bf.writeUTF("NO");					//col6 is nullable
			
			//Cell 4
			bf.writeShort(52);					//payload of row id 1
			bf.writeInt(4);						//Key for 1
			bf.writeByte(0x05);					//5 col in addition to key
			bf.writeByte(0x0C + 21);			//col2 data type
			bf.writeByte(0x0C + 10);			//col3 data type
			bf.writeByte(0x0C + 4);				//col4 data type
			bf.writeByte(0x04);					//col5 data type
			bf.writeByte(0x0C + 2);				//col6
			
			bf.writeUTF("greenwaldbase_columns");//col2 table name
			bf.writeUTF("table_name");			//col3 column name
			bf.writeUTF("TEXT");				//col4 data type
			bf.writeByte(2);					//col5 ordinal position
			bf.writeUTF("NO");					//col6 is nullable
			
			//Cell 5
			bf.writeShort(53);					//payload of row id 1
			bf.writeInt(5);						//Key for 1
			bf.writeByte(0x05);					//5 col in addition to key
			bf.writeByte(0x0C + 21);			//col2 data type
			bf.writeByte(0x0C + 11);			//col3 data type
			bf.writeByte(0x0C + 4);				//col4 data type
			bf.writeByte(0x04);					//col5 data type
			bf.writeByte(0x0C + 2);				//col6
			
			bf.writeUTF("greenwaldbase_columns");//col2 table name
			bf.writeUTF("column_name");			//col3 column name
			bf.writeUTF("TEXT");				//col4 data type
			bf.writeByte(3);					//col5 ordinal position
			bf.writeUTF("NO");					//col6 is nullable
			
			//Cell 6
			bf.writeShort(51);					//payload of row id 1
			bf.writeInt(6);						//Key for 1
			bf.writeByte(0x05);					//5 col in addition to key
			bf.writeByte(0x0C + 21);			//col2 data type
			bf.writeByte(0x0C + 9);			//col3 data type
			bf.writeByte(0x0C + 4);				//col4 data type
			bf.writeByte(0x04);					//col5 data type
			bf.writeByte(0x0C + 2);				//col6
			
			bf.writeUTF("greenwaldbase_columns");//col2 table name
			bf.writeUTF("data_type");			//col3 column name
			bf.writeUTF("TEXT");				//col4 data type
			bf.writeByte(4);					//col5 ordinal position
			bf.writeUTF("NO");					//col6 is nullable
			
			//Cell 7
			bf.writeShort(61);					//payload of row id 1
			bf.writeInt(7);						//Key for 1
			bf.writeByte(0x05);					//5 col in addition to key
			bf.writeByte(0x0C + 21);			//col2 data type
			bf.writeByte(0x0C + 16);			//col3 data type
			bf.writeByte(0x0C + 7);				//col4 data type
			bf.writeByte(0x04);					//col5 data type
			bf.writeByte(0x0C + 2);				//col6
			
			bf.writeUTF("greenwaldbase_columns");//col2 table name
			bf.writeUTF("ordinal_position");	//col3 column name
			bf.writeUTF("TINYINT");				//col4 data type
			bf.writeByte(5);					//col5 ordinal position
			bf.writeUTF("NO");					//col6 is nullable
			
			//Cell 8
			bf.writeShort(54);					//payload of row id 1
			bf.writeInt(8);						//Key for 1
			bf.writeByte(0x05);					//5 col in addition to key
			bf.writeByte(0x0C + 21);			//col2 data type
			bf.writeByte(0x0C + 11);			//col3 data type
			bf.writeByte(0x0C + 4);				//col4 data type
			bf.writeByte(0x04);					//col5 data type
			bf.writeByte(0x0C + 2);				//col6
			
			bf.writeUTF("greenwaldbase_columns");//col2 table name
			bf.writeUTF("is_nullable");			//col3 column name
			bf.writeUTF("TEXT");				//col4 data type
			bf.writeByte(6);					//col5 ordinal position
			bf.writeUTF("NO");					//col6 is nullable
					
			bf.close();
			}
			catch(Exception e5)
			{
				System.out.println(e5);
			}
	}
	
}




