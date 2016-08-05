package primary;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class GreenwaldBase {
	
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	static String prompt = "Greenwaldsql> ";
	static long pageSize = 512; 
	static long tables_rowid = 3, columns_rowid = 9;

	public static void main(String[] args) {
		try{
		introScreen();
		}catch(Exception begining)
		{
			begining.printStackTrace();
		}
		String userCommand = "";
		
		while(!userCommand.equals("exit"))
		{
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			//userCommand = userCommand.replaceAll("\\s+", " ");
			parseUserCommand(userCommand);
		}
	}

	
	public static void introScreen() throws IOException
	{
		System.out.println(line("-",80));
		System.out.println("GreenwaldBase:");
		System.out.println("Type \"help;\" to display commands");
		System.out.println(line("-",80));
		
		File file = new File("data\\greenwaldbase_tables.tbl");
		long tmpLong = 0;
		int tmpShort = 0;
		byte tmpByte = 0;
		
		if(!file.isFile())		
		{
			Create_greenwaldbase_tables();
			tables_rowid = 3;
		}
		else
		{
			RandomAccessFile f = new RandomAccessFile ("data\\greenwaldbase_tables.tbl", "r");
			
			tmpLong = FINDKEYPAGE(f,Integer.MAX_VALUE,0);
			f.seek(tmpLong + 1);
			tmpByte = f.readByte();			
			f.seek(tmpLong + 2 + (2*tmpByte));
			tmpShort = f.readShort();
			f.seek(tmpLong + tmpShort + 2);
			tables_rowid = f.readInt();
			f.close();
		}
		
		file = new File("data\\greenwaldbase_columns.tbl");
		
		if(!file.isFile())
		{
			Create_greenwaldbase_columns();
			columns_rowid = 9;
		}
		else
		{
			RandomAccessFile f = new RandomAccessFile ("data\\greenwaldbase_columns.tbl", "r");
			
			tmpLong = FINDKEYPAGE(f,Integer.MAX_VALUE,0);
			f.seek(tmpLong + 1);
			tmpByte = f.readByte();			
			f.seek(tmpLong + 2 + (2*tmpByte));
			tmpShort = f.readShort();
			f.seek(tmpLong + tmpShort + 2);
			columns_rowid = f.readInt();
			f.close();
			
			System.out.println(columns_rowid);
		}
		
		
		
		
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
		SELECT_FROM_WHERE("select table_name from greenwaldbase_tables;");
	}
	
	public static void DROP_TABLE(String usrCom)
	{
		String[] dropped_table = usrCom.split(" ");
		
		dropped_table[2] = dropped_table[2].replace(";", "");
		
		System.out.println("Dropped table: " + dropped_table[2]);
		
		File file = new File("data\\" + dropped_table[2] + ".tbl");
		
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
	
				INSERT_INTO_TABLE("INSERT INTO TABLE greenwaldbase_tables VALUES (" + tables_rowid++ + "," + table + ");"); // INSERT THE TABLE INTO greenwaldbase_tables
				
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
		
		System.out.println(usrCom);
		/*String table = usrCom.substring(18, usrCom.indexOf(" ", 18));
		
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
		*/
	}
	
	public static void UPDATE(String usrCom)
	{
		
	}
	
	public static void SELECT_FROM_WHERE(String usrCom)
	{
		{			
			usrCom = usrCom.replace(";", "");
			
			String usrComlower = usrCom.toLowerCase();
			
			String[] select, types = {""};
			String table, where, print = "";
			
			select = usrCom.substring(7, usrCom.toUpperCase().indexOf("FROM")-1).split(",");
			
			if(usrCom.toUpperCase().contains("WHERE"))
			{
			
				table = usrCom.substring(usrCom.toUpperCase().indexOf("FROM") + 4, usrCom.toUpperCase().indexOf("WHERE") - 1).trim();
			
				where = usrCom.substring(usrCom.toUpperCase().indexOf("WHERE") + 5).trim();
				
			}
			else
			{
				table = usrCom.substring(usrCom.toUpperCase().indexOf("FROM") + 4).trim();
				
				where = "NO WHERE";
			}
					
			File f= new File("data\\" + table + ".tbl");
			
			if(f.isFile())
			{
				try
				{
					if(select[0].equals("*"))  //NEEDS WORK!!!!!		Change the select array to be an array of strings with col names
					{
						select = GETCOLS(table);
					}
					else
						for(int i = 0; i < select.length; i++)
						{
							select[i]=select[i].trim();
						}
					
					int[] ordinals = new int[select.length];
					
					for(int i = 0; i < select.length; i++)
						ordinals[i] = GETORDINALITY(table, select[i]);
					
					if(!contains(ordinals, -1))
					{
					
					RandomAccessFile File = new RandomAccessFile("data\\" + table + ".tbl","rw");
					
					int numcols, numcells = 0;
					long leftmost,arraypointer = 0, pointer = 0;
					byte pointer2 = 0;
					
					if(where.toLowerCase().contains("rowid")) //WHERE is on rowid
					{
						String operator = where.substring(5).trim().substring(0, 2).trim();
						int compareto;
						
						if(where.contains("<=") || where.contains(">=") || where.contains("<>") || where.contains("!="))
							{
							operator = where.substring(5).trim().substring(0, 2).trim();
							compareto = Integer.parseInt(where.substring(where.indexOf(operator) + 2).trim());
							}
						else
						{
							operator = where.substring(5).trim().substring(0, 1).trim();
							compareto = Integer.parseInt(where.substring(where.indexOf(operator) + 1).trim());
						}
						
						if(operator.equals("<=") || operator.equals("<")) //DONE
						{
							leftmost = FINDLEFTMOSTPAGE(File,0);
							
							pointer2 = (byte) (leftmost/pageSize);
							
							int tmpInt = 0;
							
							for(int l = 0; l < select.length; l++)
								print = print + "|\t" + select[l] + "\t";
							
							print = print + " |";
							
							System.out.println(print);
							
							do{
								File.seek(pointer2 * pageSize);			//go to the start of the leftmost page
								File.readByte();						//ignore the page type
								numcells = File.readByte();				//get numcells on this page
								File.readShort();						//ignore the start of the data address
								arraypointer = File.getFilePointer();	//Save the address of the sorted array of cells
								pointer = File.readShort() + (pointer2 * pageSize);	//Save the address of the first cell in the array
								
								for(int i = 0; i < numcells; i++)		//for each cell on the page while < what we want
								{
									File.seek(pointer);			//go to the start of the cell
									File.readShort();			//ignore the payload
									tmpInt = File.readInt();	//Get the rowid
						
									if(tmpInt < compareto && operator.equals("<"))
										DISPLAYRECORD(File, pointer, ordinals);
									else if (tmpInt <= compareto && operator.equals("<="))
										DISPLAYRECORD(File, pointer, ordinals);								
									
									arraypointer += 2;
									File.seek(arraypointer);
									pointer = File.readShort() + (pointer2 * pageSize);
								}
								
								File.seek((pointer2 *pageSize) + pageSize - 1);
								pointer2 = File.readByte();							
								
							}while(tmpInt <= compareto && pointer2 != 0);
							
						}else if (operator.equals(">=") || operator.equals(">"))
						{
							leftmost = FINDKEYPAGE(File,compareto,0);
							pointer2 = (byte) (leftmost/pageSize);
							
							int tmpInt = 0;
							
							for(int l = 0; l < select.length; l++)
								print = print + "|\t" + select[l] + "\t";
							
							print = print + " |";
							
							System.out.println(print);
							
							do{
								File.seek(pointer2 * pageSize);			//go to the start of the leftmost page
								File.readByte();						//ignore the page type
								numcells = File.readByte();				//get numcells on this page
								File.readShort();						//ignore the start of the data address
								arraypointer = File.getFilePointer();	//Save the address of the sorted array of cells
								pointer = File.readShort() + (pointer2 * pageSize);	//Save the address of the first cell in the array
								
								for(int i = 0; i < numcells; i++)		//for each cell on the page while < what we want
								{
									File.seek(pointer);			//go to the start of the cell
									File.readShort();			//ignore the payload
									tmpInt = File.readInt();	//Get the rowid
						
									if(tmpInt > compareto && operator.equals(">"))
										DISPLAYRECORD(File, pointer, ordinals);
									else if (tmpInt >= compareto && operator.equals(">="))
										DISPLAYRECORD(File, pointer, ordinals);								
									
									arraypointer += 2;
									File.seek(arraypointer);
									pointer = File.readShort() + (pointer2 * pageSize);
								}
								
								File.seek((pointer2 *pageSize) + pageSize - 1);
								pointer2 = File.readByte();							
								
							}while(tmpInt <= compareto && pointer2 != 0);
							
						}else if(operator.equals("="))
						{
							leftmost = FINDKEYPAGE(File,compareto,0);
							pointer2 = (byte) (leftmost/pageSize);
							
							int tmpInt = 0;
							
							for(int l = 0; l < select.length; l++)
								print = print + "|\t" + select[l] + "\t";
							
							print = print + " |";
							
							System.out.println(print);
							
							do{
								File.seek(pointer2 * pageSize);			//go to the start of the leftmost page
								File.readByte();						//ignore the page type
								numcells = File.readByte();				//get numcells on this page
								File.readShort();						//ignore the start of the data address
								arraypointer = File.getFilePointer();	//Save the address of the sorted array of cells
								pointer = File.readShort() + (pointer2 * pageSize);	//Save the address of the first cell in the array
								
								for(int i = 0; i < numcells; i++)		//for each cell on the page while < what we want
								{
									File.seek(pointer);			//go to the start of the cell
									File.readShort();			//ignore the payload
									tmpInt = File.readInt();	//Get the rowid
						
									if(tmpInt == compareto)
										DISPLAYRECORD(File, pointer, ordinals);							
									
									arraypointer += 2;
									File.seek(arraypointer);
									pointer = File.readShort() + (pointer2 * pageSize);
								}
								
								File.seek((pointer2 *pageSize) + pageSize - 1);
								pointer2 = File.readByte();							
								
							}while(tmpInt == compareto && pointer2 != 0);
							
						}else
						{
							leftmost = FINDLEFTMOSTPAGE(File,0);
							
							pointer2 = (byte) (leftmost/pageSize);
							
							int tmpInt = 0;
							
							for(int l = 0; l < select.length; l++)
								print = print + "|\t" + select[l] + "\t";
							
							print = print + " |";
							
							System.out.println(print);
							
							do{
								File.seek(pointer2 * pageSize);			//go to the start of the leftmost page
								File.readByte();						//ignore the page type
								numcells = File.readByte();				//get numcells on this page
								File.readShort();						//ignore the start of the data address
								arraypointer = File.getFilePointer();	//Save the address of the sorted array of cells
								pointer = File.readShort() + (pointer2 * pageSize);	//Save the address of the first cell in the array
								
								for(int i = 0; i < numcells; i++)		//for each cell on the page while < what we want
								{
									File.seek(pointer);			//go to the start of the cell
									File.readShort();			//ignore the payload
									tmpInt = File.readInt();	//Get the rowid
						
									if(tmpInt != compareto)
										DISPLAYRECORD(File, pointer, ordinals);							
									
									arraypointer += 2;
									File.seek(arraypointer);
									pointer = File.readShort() + (pointer2 * pageSize);
								}
								
								File.seek((pointer2 *pageSize) + pageSize - 1);
								pointer2 = File.readByte();							
								
							}while(tmpInt != compareto && pointer2 != 0);
							
						}
					}
					else if(!where.equals("NO WHERE")) //WHERE is not on rowid
					{
						String compcol = where.substring(0, where.indexOf("=")).trim();			
						
						where = where.substring(where.indexOf("=") + 1).trim();
											
						leftmost = FINDLEFTMOSTPAGE(File, 0);
											
						pointer2 = (byte) (leftmost/pageSize);
						
						for(int l = 0; l < select.length; l++)
							print = print + "|\t" + select[l] + "\t";
						
						print = print + " |";
						
						System.out.println(print);
											
						do
						{
						File.seek(pointer2 * pageSize);	
							
						File.readByte();
						
						numcells = File.readByte();
						pointer = pageSize*pointer2 + File.readShort();					
						
						for(int k = 0; k < numcells; k++)
						{
							pointer = (int)DISPLAYRECORDWHEREEQUAL(File, pointer, ordinals, GETORDINALITY(table,compcol), where);		
						}
						
						pointer2 = File.readByte();
						}while(pointer2 != 0);
					}
					else //NO WHERE
					{
						leftmost = FINDLEFTMOSTPAGE(File, 0);
											
						pointer2 = (byte) (leftmost/pageSize);
						
						for(int l = 0; l < select.length; l++)
							print = print + "|\t" + select[l] + "\t";
						
						print = print + " |";
						
						System.out.println(print);
						
						do
						{
						File.seek(pointer2 * pageSize);	
							
						File.readByte();
						
						numcells = File.readByte();
						pointer = pageSize*pointer2 + File.readShort();					
						
						for(int k = 0; k < numcells; k++)
						{
							pointer = (int)DISPLAYRECORD(File, pointer, ordinals);
							
						}
						
						pointer2 = File.readByte();
						}while(pointer2 != 0);
						
					}
					}
					else
						System.out.println("Given Column missing from table");
					
					
				}catch (Exception e4)
				{
					System.out.println("Caught Exception in SELECT_FROM_WHERE Function:");
					e4.printStackTrace();
				}
			}
			else
				System.out.println("Error: The table \"" + table + "\" does not exist.");
		}
		
		
	}
	
	public static void DELETE_FROM_WHERE(String usrCom)
	{
		System.out.println("Need to write DELETE_FROM_WHERE");
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
	public static long FINDLEFTMOSTPAGE(RandomAccessFile file, long pos) throws IOException //returns the #bytes ofset from beginning of file of the leftmost page
	{
		file.seek(pos);
						
		if(file.readByte() == 0x0D)
			return pos;
		else
		{
			file.readByte();
			file.readShort();
			file.readInt();
			file.seek(file.readShort() + pos);
			long tmplong = file.readInt() * pageSize;
			
			return 	FINDLEFTMOSTPAGE(file,tmplong);

		}
	}
	
	public static String GETDATATYPE(String table, int ord) throws IOException //WORKS! returns the cardinality of the colname in table
	{
		RandomAccessFile File = new RandomAccessFile("data\\greenwaldbase_columns.tbl", "r");
		
		byte tmpByte = 0;
		int pagepointer = 0, numcells, pointer, tmpInt = 0;
		
		String tmptable = "", tmpString = "";
		
		long leftmost = FINDLEFTMOSTPAGE(File, 0), arraypointer;
		
		pagepointer = (int) (leftmost/pageSize);
				
		do
		{
			File.seek(pagepointer * pageSize);
			File.readByte();						//throw away the page type
			numcells = File.readByte();				//get num cells
			File.readShort();						//Jump over start of data address
			arraypointer = (File.getFilePointer());
			pointer = (int) ((pagepointer*pageSize) + File.readShort());		//get address of first record
					
			for(int i = 0; i < numcells; i++)
			{				
				tmptable = "";
				tmpInt = 0;
				
				File.seek(pointer);

				File.readShort();	//Skip over payload
				File.readInt();		//Skip over rowid
				File.readByte();	//Skip over num cols
				File.readByte();	//Skip over datatype for tablename
				File.readByte();	//Skip over datatype for colname
				File.readByte();	//Skip over datatype for datatype
				File.readByte();	//Skip over datatype for ordpos
				File.readByte();	//Skip over datatype for isnull
								
				tmptable = File.readUTF();	//Get the table string
				File.readUTF();				//Skip the column_name
				tmpString = File.readUTF();	//Get data_type
				tmpByte = File.readByte();	//Get the ordinal position
												
				if(tmptable.equals(table) && tmpByte == ord)				//If its the correct table
				{
					return tmpString;
				}

				File.seek(arraypointer + 2);	//find next address on list
				arraypointer = File.getFilePointer();
				pointer = (int) ((pagepointer*pageSize) + File.readShort());				//get address
				
			}
			
			File.seek(pagepointer * pageSize - 1 + pageSize);		//Find next page in the line
			
			pagepointer = File.readByte();				//store that page
							
		}
		while (pagepointer != 0);
		
		return "ERROR";
	}
	
	public static int GETORDINALITY(String table, String colname) throws IOException //WORKS! returns the cardinality of the colname in table
	{
		RandomAccessFile File = new RandomAccessFile("data\\greenwaldbase_columns.tbl", "r");
		
		int pagepointer = 0, numcells, pointer;
		
		String tmpcol = "", tmptable = "";
		
		long leftmost = FINDLEFTMOSTPAGE(File, 0), arraypointer;
		
		pagepointer = (int) (leftmost/pageSize);
				
		do
		{
			File.seek(pagepointer * pageSize);
			File.readByte();						//throw away the page type
			numcells = File.readByte();				//get num cells
			File.readShort();						//Jump over start of data address
			arraypointer = (File.getFilePointer());
			pointer = (int) ((pagepointer*pageSize) + File.readShort());		//get address of first record
					
			for(int i = 0; i < numcells; i++)
			{				
				tmptable = "";
				tmpcol = "";
				
				File.seek(pointer);

				File.readShort();		//Skip over payload
				File.readInt();			//Skip over rowid
				File.readByte();		//Skip over num cols
				File.readByte();		//Skip over datatype for tablename
				File.readByte();		//Skip over datatype for colname
				File.readByte();		//Skip over datatype for datatype
				File.readByte();		//Skip over datatype for ordpos
				File.readByte();		//Skip over datatype for isnull
								
				tmptable = File.readUTF();	//Get the table string
												
				if(tmptable.equals(table))				//If its the correct table
				{
					tmpcol = File.readUTF();			//Get the col
						if(tmpcol.equals(colname))		//If its the right col
						{
							File.readUTF();				//skip the data type
							return File.readByte();		//return the ordinal position
						}
				}

				File.seek(arraypointer + 2);	//find next address on list
				arraypointer = File.getFilePointer();
				pointer = (int) ((pagepointer*pageSize) + File.readShort());				//get address
				
			}
			
			File.seek(pagepointer * pageSize - 1 + pageSize);		//Find next page in the line
			
			pagepointer = File.readByte();				//store that page
							
		}
		while (!tmpcol.equals(colname) && pagepointer != 0);
		
		return -1;
	}
	
	public static long DISPLAYRECORD(RandomAccessFile file, long pointer, int[] ordinals) throws IOException
	{		
		Arrays.sort(ordinals);
		
		String print = "";
				
		file.seek(pointer);						//Go to start of record
		short payload = file.readShort();		//Store payload
		int key = file.readInt();				//Store Key
		
		if(contains(ordinals, 1))
			print = "|\t" + key + "\t";
		
		byte numcol_available = file.readByte();
		
		if(numcol_available + 1 < ordinals.length)
		{
			System.out.println("ERROR: you are asking for to many columns.");
			IOException e = new IOException();
			throw e;
		}
		
		byte[] types = new byte[numcol_available];
		
		for(int i = 0; i < numcol_available; i++)
			types[i] = file.readByte();
		
		file.seek(pointer + 7 + numcol_available);
		
		long tmp;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'_'HH:mm:ss");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
		Date d;
		
		for(int j = 0; j < numcol_available; j++)
		{
			switch(types[j])
			{
			case 0:		//1 byte Null
				file.readByte();
				if(contains(ordinals, j+2))
					print = print + "|\t" + "NULL" + "\t";
				break;
			case 1:		//2 byte Null
				file.readShort();
				if(contains(ordinals, j+2))
					print = print + "|\t" + "NULL" + "\t";
				break;
			case 2:		//4 byte Null
				file.readInt();
				if(contains(ordinals, j+2))
					print = print + "|\t" + "NULL" + "\t";
				break;
			case 3:		//8 byte Null
				file.readLong();
				if(contains(ordinals, j+2))
					print = print + "|\t" + "NULL" + "\t";
				break;
			case 4:
				if(contains(ordinals, j+2))
					print = print + "|\t" + file.readByte() + "\t";
				else
					file.readByte();
				break;
			case 5:
				if(contains(ordinals, j+2))
					print = print + "|\t" + file.readShort() + "\t";
				else
					file.readShort();
				break;
			case 6:
				if(contains(ordinals, j+2))
					print = print + "|\t" + file.readInt() + "\t";
				else
					file.readInt();
				break;
			case 7:
				if(contains(ordinals, j+2))
					print = print + "|\t" + file.readLong() + "\t";
				else
					file.readLong();
				break;
			case 8:
				if(contains(ordinals, j+2))
					print = print + "|\t" + file.readFloat() + "\t";
				else
					file.readFloat();
				break;
			case 9:
				if(contains(ordinals, j+2))
					print = print + "|\t" + file.readDouble() + "\t";
				else
					file.readDouble();
				break;
			case 10:
				tmp = file.readLong();
				if(contains(ordinals, j+2))
				{
					d = new Date(TimeUnit.SECONDS.toMillis(tmp));				
					print = print + "|\t" + sdf.format(d) + "\t";
				}
				break;
			case 11:
				tmp = file.readLong();
				if(contains(ordinals, j+2))
				{
					d = new Date(TimeUnit.SECONDS.toMillis(tmp));				
					print = print + "|\t" + sdf2.format(d) + "\t";
				}
				break;
			default:
				if(contains(ordinals,j+2))
					print = print + "|\t" + file.readUTF() + "\t";
				else
					file.readUTF();
				break;
				
			}
		}
		
		print = print + " |";
	
		System.out.println(print);
		
		pointer = pointer + 6 + payload;
		
		return pointer;
	}
	
	public static boolean contains(int[] array, int num)
	{
		for(int i = 0; i < array.length; i++)
			if(array[i] == num)
				return true;
		
		return false;
	}
	
	public static long DISPLAYRECORDWHEREEQUAL(RandomAccessFile file, long pointer, int[] ordinals, int ord, String where) throws IOException
	{
		boolean whereistrue = false;
		int key;
		byte numcol_available;
		byte[] types;
		long tmp;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'_'HH:mm:ss");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
		Date d;
		
		where = where.replace("\"", "");
		
		Arrays.sort(ordinals);
		
		String print = "", tmpstring = "";
		
		file.seek(pointer);						//Go to start of record
		short payload = file.readShort();		//Store payload
		if(ord == 1)							//if where is on rowid
		{
			key = file.readInt();				//Get the rowid
			if(key == Integer.parseInt(where))	//check it against the key we got
				whereistrue = true;				//we found the right record
		}
		else
		{
			file.readInt();						//throw the row id away
			numcol_available = file.readByte();	//get num cols
			types = new byte[numcol_available];	
			
			for(int i = 0; i < numcol_available; i++)	//Get all the data types
				types[i] = file.readByte();
			
			file.seek(pointer + 7 + numcol_available);	//move to the first data entry
			
			loop:
			for(int j = 0; j < numcol_available; j++)	
			{
				switch(types[j])
				{
				case 0:		//1 byte Null
					file.readByte();
					if(ord == j+2 && where.toLowerCase().equals("\"null\""))
					{
						whereistrue = true;
						break loop;
					}
					break;
				case 1:		//2 byte Null
					file.readShort();
					if(ord == j+2 && where.toLowerCase().equals("\"null\""))
					{
						whereistrue = true;
						break loop;
					}					
					break;
				case 2:		//4 byte Null
					file.readInt();
					if(ord == j+2 && where.toLowerCase().equals("\"null\""))
					{
						whereistrue = true;
						break loop;
					}
					break;
				case 3:		//8 byte Null
					file.readLong();
					if(ord == j+2 && where.toLowerCase().equals("\"null\""))
					{
						whereistrue = true;
						break loop;
					}
					break;
				case 4:
					if(ord == j+2)
						if(Byte.parseByte(where) == file.readByte())
						{
							whereistrue = true;
							break loop;
						}
						else;
					else
						file.readByte();
					break;
				case 5:
					if(ord == j+2)
						if(Short.parseShort(where) == file.readShort())
						{
							whereistrue = true;
							break loop;
						}
						else;
					else
						file.readShort();
					break;
				case 6:
					if(ord == j+2)
						if(Integer.parseInt(where) == file.readInt())
						{
							whereistrue = true;
							break loop;
						}
						else;
					else
						file.readInt();
					break;
				case 7:
					if(ord == j+2)
						if(Long.parseLong(where) == file.readLong())
						{
							whereistrue = true;
							break loop;
						}
						else;
					else
						file.readLong();
					break;
				case 8:
					if(ord == j+2)
						if(Float.parseFloat(where) == file.readFloat())
						{
							whereistrue = true;
							break loop;
						}
						else;
					else
						file.readFloat();
					break;
				case 9:
					if(ord == j+2)
						if(Double.parseDouble(where) == file.readDouble())
						{
							whereistrue = true;
							break loop;
						}
						else;
					else
						file.readDouble();
					break;
				case 10:
					tmp = file.readLong();
					if(ord == j+2)
					{
						d = new Date(TimeUnit.SECONDS.toMillis(tmp));				
						if(where.equals(sdf.format(d)))
						{
							whereistrue = true;
							break loop;
						}
					}
					break;
				case 11:
					tmp = file.readLong();
					if(ord == j+2)
					{
						d = new Date(TimeUnit.SECONDS.toMillis(tmp));				
						if(where.equals(sdf2.format(d)))
						{
							whereistrue = true;
							break loop;
						}
					}
					break;
				default:
					tmpstring = file.readUTF();
					if(ord == j+2 && where.equals(tmpstring))
					{
						whereistrue = true;
						break loop;
					}
					break;
					
				}
			}
			
			
		}
		
		if(whereistrue)
			DISPLAYRECORD(file, pointer, ordinals);
		
		pointer = pointer + 6 + payload;
		
		return pointer;
	}
	
	public static String[] GETCOLS(String table) throws IOException
	{
		String[] ret;
		String tmpString = "";
		long pointer = 0, leftmost = 0;
		byte pointer2 = 0;
		int numcells = 0, cellsize = 0;
		
		table = table.toLowerCase();
		
		RandomAccessFile File = new RandomAccessFile ("data\\greenwaldbase_columns.tbl" , "r");
		
		leftmost = FINDLEFTMOSTPAGE(File, 0);		//Find the leftmost page address
		
		pointer2 = (byte) (leftmost/pageSize);		// pointer2 is leftmost page #

		do
		{
		File.seek(pointer2 * pageSize);				//seek to the address of the leftmost page #
			
		File.readByte();		//throw away page type
		
		numcells = File.readByte();		//Get num cells on this page
		pointer = pageSize*pointer2 + File.readShort();		// pointer is now the address of the first cell on the page
		
		for(int i = 0; i < numcells; i++)	//for each cell on page
		{
			File.seek(pointer);					//seek to the start of the cell
			cellsize = File.readShort() + 6;	//get the total size of the cell
			File.seek(pointer + 12);			//seek to the start of info in the cell
			pointer = pointer + cellsize;		//move the pointer to the next cell
			if(File.readUTF().toLowerCase().equals(table))		//read a string from the file. if it equals the table name
				tmpString = tmpString + File.readUTF() + ",";	//add the col name of the cell to tmpString + a comma		
		}
		
		
		
		pointer2 = File.readByte();
		}while(pointer2 != 0);
		
		tmpString = tmpString.substring(0, tmpString.length()-1);
		
		ret = tmpString.split(",");
		
		for(int i = 0; i < ret.length; i++)
			ret[i]=ret[i].trim();	
		
		return ret;
	}
	
	public static long FINDKEYPAGE(RandomAccessFile File, int key, long pointer) throws IOException
	{
		File.seek(pointer);
		
		if(File.readByte() == 0x0D)
			return pointer;
		else
		{
			int tmpInt, page;

			File.readByte();
			File.readShort();
			page = File.readInt();
			File.seek(pointer + pageSize - 4);
			tmpInt = File.readInt();
			
			if(key <= tmpInt)		//The key is in the rightmost page if it exists
			{
				File.seek(pointer + 8);					//go to offset of storage of offset of first cell
				File.seek(File.readShort() + pointer);	//
				
				do{
					page = File.readInt();
					tmpInt = File.readInt();
				
				}while(key > tmpInt);
				
			}
			
			pointer = page * pageSize;
			
			return FINDKEYPAGE(File, key, pointer);
		
		}
	}
	
}




