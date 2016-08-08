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
			if (commandTokens[1].equals("into") && commandTokens[2].equals("table")/* && commandTokens[4].equals("values")*/)
			{
				INSERT_INTO_TABLE(userCommand);
			}
			else
				System.out.println("Got UKNOWN command: " + userCommand);
			break;
			
		case "update":
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
		String table = usrCom.substring(13, usrCom.indexOf("(", 13)).trim();
		
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
				columns[i] = columns[i].trim();
						
			RandomAccessFile binaryFile;
			
			try {
				binaryFile = new RandomAccessFile("data\\" + table + ".tbl", "rw");
				
				binaryFile.setLength(0);
				binaryFile.setLength(pageSize);
				binaryFile.seek(0);
				binaryFile.writeByte(0x0D);
				binaryFile.readByte();
				binaryFile.writeShort((int) (pageSize - 1));
				
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
	
	public static void INSERT_INTO_TABLE(String usrCom)
	{
		System.out.println(usrCom);
		
		try{
			
			String table = usrCom.substring(18, usrCom.indexOf(" ", 18));
					
			File file = new File("data\\" + table + ".tbl");
			
			usrCom = usrCom.replaceFirst("(?i)values(?-i)", "VALUES").replace(")", "");
			
			String[] tmp2 = usrCom.split("VALUES");
					
			if(file.isFile())
			{
			tmp2[1] = tmp2[1].replace("(", "").replace(");","");
			String[] tmp = tmp2[1].split(",");
			for(int i = tmp.length; i < GETNUMCOLUMNS(table); i++)
				tmp2[1] = tmp2[1] + ",NULL"; 
				
			String[] values = tmp2[1].split(",");
			
			for(int i = 0; i < values.length; i++)
			{
				values[i] = values[i].trim().replace("\"", "");
				if(values[i].toLowerCase().contains("null"))
					values[i] = "NULL";
			}
							
			int key = Integer.parseInt(values[0]);
			
			if(KEYEXISTS(table,key))
			{
				System.out.println("Duplicate Key Error");
				return;
			}
			
			//String type = SELECT_FROM_WHERE_tostring("SELECT DATA_TYPE FROM greenwaldbase_tables WHERE table_name=\"" + table + "\";");
				
				for(int i = 0; i < values.length;i++)
					if(!ISNULLABLE(table,(i+1)) && values[i].equals("NULL"))
					{
						System.out.println("ERROR: NULL values in a non nullable column");
						return;
					}
			
			short payload = 1;	//Every record has 1 in payload for num cols
			
			String[] datatypes = new String [values.length];
			//byte[] databytes = new byte [values.length];
			
			for(int i = 0; i < values.length ;i++)		//determine the datatypes
			{
				datatypes[i] = GETDATATYPE(table,i+1);
				
			}
			
			for(int i = 1; i < values.length; i++)	//determine the size of the payload
				{
					payload++;
					switch(datatypes[i])
					{
					case "TINYINT":
						payload += 1;
						break;
					case "SMALLINT":
						payload += 2;
						break;
					case "INT":
						payload += 4;
						break;
					case "BIGINT":
						payload += 8;
						break;
					case "REAL":
						payload += 4;
						break;
					case "DOUBLE":
						payload += 8;
						break;
					case "DATETIME":
						payload += 8;
						break;
					case "DATE":
						payload += 8;
						break;
						default:
							payload += 2 + values[i].length();
							break;
					}
				}
			
			if(payload + 11 > pageSize)
			{
				System.out.println("This record is to large for the Page Size.");
				return;
			}
				
			long pointer = 0, pagePointer = 0, arraypointer = 0;
			int tempKey = 0, numcells = 0;
			short tmparray = 0, tmparray2 = 0, startPointer = 0;
				
			RandomAccessFile f = new RandomAccessFile("data\\" + table + ".tbl", "rw");
			
			pagePointer = FINDKEYPAGE(f,key,0);		//Find the page the record should go on		
			f.seek(pagePointer);					//Seek to that page		
			f.readByte();							//throw away page type		
			numcells = f.readByte();				//get numcells
			pointer = f.readShort();				//get offset of start of content
						
			for(int i = 0; i < numcells; i++)		//Skip to the start of free space after address array
				f.readShort();
			
			System.out.println("pointer: " + pointer + " pagePointer: "+ pagePointer + " filepointer: " + f.getFilePointer() + " payload: " + payload);
					
			while((pointer + pagePointer) - f.getFilePointer() < payload + 8)	//While there is NOT enough space to insert into this page
			{
				LEAFSPLIT(f,pagePointer);										//Split the page
				pagePointer = FINDKEYPAGE(f,key,0);								//Find the new page the record should go on		
				f.seek(pagePointer);											//Seek to that page
				f.readByte();													//throw away page type
				numcells = f.readByte();										//get numcells
				pointer = f.readShort();										//get offset of start of content
				
				for(int i = 0; i < numcells; i++)								//Skip to the start of free space after address array
					f.readShort();
			}							
			
			pagePointer = FINDKEYPAGE(f,key,0);
			
			f.seek(pagePointer);								//Go to the page where the record will be inserted
			f.readByte();										//throw away page type
			numcells = f.readByte();							//get numcells
			f.seek(f.getFilePointer() - 1);
			f.writeByte(++numcells);							//update numcells
			pointer = f.readShort();							//get address of start of offset
			startPointer = (short) ((pointer) - (payload + 6));	//offset of the start of the record is the offset of the first record minus (payload + 6)
			f.seek(f.getFilePointer() - 2);
			f.writeShort((int) (pointer - (payload + 6)));
			arraypointer = f.getFilePointer();					//where we are in the address array
			tmparray2 = startPointer;
			
			
			for(int i = 0; i < numcells + 1; i++)
			{
				tmparray = f.readShort();							//address of smallest record
				f.seek(pagePointer + tmparray);						//seek to the page number times page size + offset of the smallest record
				f.readShort();										//throw away the payload
				tempKey = f.readInt();								//tempKey is now the smallest key on the page
				if(key < tempKey)
					{
						f.seek(arraypointer);
						f.writeShort(tmparray2);
						tmparray2 = tmparray;
						arraypointer += 2;
						f.seek(arraypointer);
					}
					
				else
				{
					arraypointer += 2;
					f.seek(arraypointer);
				}
					
			}
			
			f.seek(pagePointer + startPointer);
			System.out.println("startpointer: "+startPointer+" pagePointer: " + pagePointer);
			f.writeShort(payload);
			f.writeInt(key);
			f.writeByte(values.length - 1);
			
			for(int i = 1; i < values.length; i++)
			{
				switch(datatypes[i])
				{
				case "TINYINT":
					if(values[i].equals("NULL"))
						f.writeByte(0x00);
					else
						f.writeByte(0x04);
					break;
				case "SMALLINT":
					if(values[i].equals("NULL"))
						f.writeByte(0x01);
					else
						f.writeByte(0x05);
					break;
				case "INT":
					if(values[i].equals("NULL"))
						f.writeByte(0x02);
					else
						f.writeByte(0x06);
					break;
				case "BIGINT":
					if(values[i].equals("NULL"))
						f.writeByte(0x03);
					else
						f.writeByte(0x07);
					break;
				case "REAL":
					if(values[i].equals("NULL"))
						f.writeByte(0x02);
					else
						f.writeByte(0x08);
					break;
				case "DOUBLE":
					if(values[i].equals("NULL"))
						f.writeByte(0x03);
					else
						f.writeByte(0x09);
					break;
				case "DATETIME":
					if(values[i].equals("NULL"))
						f.writeByte(0x03);
					else
						f.writeByte(0x0A);
					break;
				case "DATE":
					if(values[i].equals("NULL"))
						f.writeByte(0x03);
					else
						f.writeByte(0x0B);
					break;
					default:
						if(values[i].equals("NULL"))
							f.writeByte(0x0C + 0x04);
						else
							f.writeByte(0x0C + values[i].length());
						break;
				}
			}
			
			f.seek(startPointer + 6 + values.length);	//Seek to the start of the record plus the distance to the first data storage
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'_'HH:mm:ss");
			SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
			Date d;
			
			for(int i = 1; i < values.length; i++)
			{
				switch(datatypes[i])
				{
				case "TINYINT":
					if(values[i].equals("NULL"))
						f.writeByte(0);
					else
						f.writeByte(Byte.parseByte(values[i]));
					break;
				case "SMALLINT":
					if(values[i].equals("NULL"))
						f.writeShort(0);
					else
						f.writeShort(Short.parseShort(values[i]));
					break;
				case "INT":
					if(values[i].equals("NULL"))
						f.writeInt(0);
					else
						f.writeInt(Integer.parseInt(values[i]));
					break;
				case "BIGINT":
					if(values[i].equals("NULL"))
						f.writeLong(0);
					else
						f.writeLong(Long.parseLong(values[i]));
					break;
				case "REAL":
					if(values[i].equals("NULL"))
						f.writeFloat(0);
					else
						f.writeFloat(Float.parseFloat(values[i]));
					break;
				case "DOUBLE":
					if(values[i].equals("NULL"))
						f.writeDouble(0);
					else
						f.writeDouble(Double.parseDouble(values[i]));
					break;
				case "DATETIME":
					if(values[i].equals("NULL"))
						f.writeLong(0);
					else
					{
						d = new Date();
						d = sdf.parse(values[i]);
						f.writeLong(d.getTime());
					}
					break;
				case "DATE":
					if(values[i].equals("NULL"))
						f.writeLong(0);
					else
					{
						d = new Date();
						d = sdf2.parse(values[i]);
						f.writeLong(d.getTime());
					}				break;
					default:
						if(values[i].equals("NULL"))
							f.writeUTF("NULL");
						else
						{
							System.out.println("INSERT STRING: " + values[i].replace(";", ""));
							f.writeUTF(values[i].replace(";", ""));
						}
						break;
				}
			}

			
			f.close();
			
			
			}
			else
				System.out.println("The table: \"" + table + "\" could not be found.");
			}
			catch(Exception e2)
			{
				e2.printStackTrace();		
			}		
	}
	
	public static void LEAFSPLIT(RandomAccessFile file, long page0) throws IOException	//Give file and pointer to leaf you want split	
	{																						//KEEP THE LIST CHAIN GOING HERE
		byte		numcells = 0, numcells1 = 0, numcells2 = 0, nextinlist;
		byte[][]	records1, records2;
		short		startoffset1 = (short) pageSize, startoffset2 = (short) pageSize;
		short[]		arraylist1, arraylist2;
		int			splitkey = 0;
		long		arraypointer = 0, parentPointer = 0, page1 = 0, arraypointer1 = 0, arraypointer2 = 0;
		
		file.seek(page0);									//seek to start of leaf to split
		file.readByte();									//skip page type
		numcells = file.readByte();							//get num cells on the page
		numcells1 = (byte) Math.ceil((float)numcells/2);	//numcells in the first page is the larger of the two if not div by 2
		numcells2 = (byte) (numcells/2);					//numcells in second page is the smaller of the two
		file.readShort();						//get offset of start of content
		arraypointer = file.getFilePointer();				//get address of array start
		
		parentPointer = FINDPARENT(file, file.readInt(), 0);
				
		records1 = new byte[numcells1][];
		records2 = new byte[numcells2][];
		
		arraylist1 = new short[numcells1];
		arraylist2 = new short[numcells2];
		
		file.seek(page0);
		file.readByte();
		file.readByte();
		file.readShort();
		for(int i = 0; i < numcells1 - 1; i++)
			file.readShort();
		file.seek(file.readShort() + page0);
		file.readShort();
		splitkey = file.readInt();
		
		for(int i = 0; i < numcells; i++)					//calculating and storing the offset of start of addresses for each page
		{
			file.seek(arraypointer + 2*i);
			if(i < numcells1)							//this cell will be going into the first
			{
				arraylist1[i] = file.readShort();
				file.seek(page0 + arraylist1[i]);
				arraylist1[i] = file.readShort();		//store the payload of each cell
				startoffset1 = (short) (startoffset1 - (arraylist1[i] + 6)); 		//The start of the first record will be pushed back from the end by the payload + 6
				records1[i] = new byte[arraylist1[i] + 6];
				file.seek(file.getFilePointer() - 2);
				file.readFully(records1[i]);
			}else										//this cell will be going into the second
			{
				arraylist2[i-numcells1] = file.readShort();
				file.seek(page0 + arraylist2[i-numcells1]);
				arraylist2[i-numcells1] = file.readShort();		//store the payload of each cell
				startoffset2 = (short) (startoffset2 - (arraylist2[i-numcells1] + 6)); 		//The start of the first record will be pushed back from the end by the payload + 6
				records2[i-numcells1] = new byte[arraylist2[i-numcells1] + 6];
				file.seek(file.getFilePointer() - 2);
				file.readFully(records2[i-numcells1]);
			}
		}
		
		file.seek(page0 + (pageSize-1));
		nextinlist = file.readByte();
		
		startoffset1--;
		startoffset2--;		
		
		if(parentPointer == -1)		//This is the first split. We are on page 0
		{
			file.setLength(3*pageSize);
			file.seek(pageSize);
			file.writeByte(0x0d);				//page is now a leaf node
			file.writeByte(numcells1);			//this is the first new page
			file.writeShort(startoffset1);		//write the start of offset for page 1
			file.writeShort(startoffset1);
			arraylist1[0] = (short) (arraylist1[0] + startoffset1+6);
			file.writeShort(arraylist1[0]);
			for(int i = 1; i < numcells1 - 1; i++)
			{
				arraylist1[i] = (short) (arraylist1[i] + arraylist1[i-1] + 6);
				file.writeShort(arraylist1[i]);
			}
			
			arraypointer1 = pageSize + 4;
			
			for(int i = 0; i < numcells1; i++)
			{
				file.seek(arraypointer1 + i*2);
				file.seek(file.readShort() + pageSize);
				file.write(records1[i]);
			}
			
			file.writeByte(0x02);
			
			file.seek(2*pageSize);
			file.writeByte(0x0d);				//page is now a leaf node
			file.writeByte(numcells2);			//this is the first new page
			file.writeShort(startoffset2);		//write the start of offset for page 1
			file.writeShort(startoffset2);
			arraylist2[0] = (short) (arraylist2[0] + startoffset2+6);
			file.writeShort(arraylist2[0]);
			for(int i = 1; i < numcells2 - 1; i++)
			{
				arraylist2[i] = (short) (arraylist2[i] + arraylist2[i-1] + 6);
				file.writeShort(arraylist2[i]);
			}
			
			arraypointer2 = 2*pageSize + 4;
			
			for(int i = 0; i < numcells2; i++)
			{
				file.seek(arraypointer2 + i*2);
				file.seek(file.readShort() + 2*pageSize);
				file.write(records2[i]);
			}
			
			file.writeByte(0x00);
			
			file.seek(0);
			file.writeByte(0x05);
			file.writeByte(0x01);
			file.writeShort((int) (pageSize - 8));
			file.writeInt(2);
			file.writeShort((int) (pageSize - 8));
			file.seek(pageSize - 8);
			file.writeInt(1);
			file.writeInt(splitkey);
			
		}
		else						//This is not the first split and we can proceed
		{			
			file.setLength(file.length() + pageSize);		//add another page
			page1 = file.length() - pageSize;
			file.seek(page1);				//go to the new page
			file.writeByte(0x0d);			//its a leaf page
			file.writeByte(numcells2);		//number of cells in page 2
			file.writeShort(startoffset2);	//set the offset of page 2
			file.writeShort(startoffset2);	//smallest cell offset
			arraylist2[0] = (short) (arraylist2[0] + startoffset2+6);	//increase the offset
			file.writeShort(arraylist2[0]);
			for(int i = 1; i < numcells2 - 1; i++)
			{
				arraylist2[i] = (short) (arraylist2[i] + arraylist2[i-1] + 6);
				file.writeShort(arraylist2[i]);
			}
			
			arraypointer2 = page1 + 4;
			
			for(int i = 0; i < numcells2; i++)
			{
				file.seek(arraypointer2 + i*2);
				file.seek(file.readShort() + page1);
				file.write(records2[i]);
			}
			
			file.writeByte(nextinlist);		//NEEDS TO POINT TO page0 OLD NEXT PAGE
			
			file.seek(page0);
			file.writeByte(0x0d);				//page is now a leaf node
			file.writeByte(numcells1);			//this is the first new page
			file.writeShort(startoffset1);		//write the start of offset for page 1
			file.writeShort(startoffset1);
			arraylist1[0] = (short) (arraylist1[0] + startoffset1+6);
			file.writeShort(arraylist1[0]);
			for(int i = 1; i < numcells1 - 1; i++)
			{
				arraylist1[i] = (short) (arraylist1[i] + arraylist1[i-1] + 6);
				file.writeShort(arraylist1[i]);
			}
			
			arraypointer1 = page0 + 4;
			
			for(int i = 0; i < numcells1; i++)
			{
				file.seek(arraypointer1 + i*2);
				file.seek(file.readShort() + page0);
				file.write(records1[i]);
			}
			
			file.writeByte((byte)(page1/pageSize));
			
			SENDTOPARENT(file,parentPointer, page0,splitkey,page1);
						
		}
		
	}
	
	public static void SENDTOPARENT(RandomAccessFile file, long parentpointer, long page0, int key, long page1) throws IOException
	{		
		boolean	done = false;
		byte	numcells = 0;
		short	offset = 0;
		int 	rightmost = 0, tmpkey1 = 0, tmppage1 = 0;
		
		file.seek(parentpointer);		//start at the parent
		file.readByte();				//skip page type
		numcells = file.readByte();		//get numcells
		offset = file.readShort();		//get offset
		
		if((10 + 8 * (numcells + 1) > pageSize))
				//parentpointer = SPLITPARENT(file, parentpointer, key);		//needs to return the long pointer to the page the key needs to go on
		
		file.seek(parentpointer);								//start of page
		file.readByte();										//ignore type
		numcells = file.readByte();								//get numcells
		file.seek(file.getFilePointer() - 1);
		numcells++;
		file.writeByte(numcells);								//increament numcells
		offset = file.readShort();								//get offset
		file.seek(file.getFilePointer() - 2);
		offset = (short) (offset - 8);
		file.writeShort(offset);
		rightmost = file.readInt();								//get rightmost
		file.writeShort(offset);
		file.seek(offset + parentpointer + 8);					//go to the start of the array
				
		for(int i = 0; i < numcells && !done; i++)
		{
			tmppage1 = file.readInt();
			tmpkey1 = file.readInt();
			
			file.seek(file.getFilePointer() - 16);
			file.writeInt(tmppage1);
			if(tmpkey1 > key)
			{
				file.writeInt(key);
				file.writeInt((int) (page1/pageSize));
				done = true;
			}
			else
			{
				file.writeInt(tmpkey1);
				file.readInt();
				file.readInt();
			}
		}
		
		if(!done)
		{
			file.seek(parentpointer + pageSize - 8);
			file.writeInt(rightmost);
			file.writeInt(key);
			file.seek(parentpointer + 4);
			file.writeInt((int) (page1/pageSize));
		}
		
	}
	
	public static long FINDPARENT(RandomAccessFile file, int key, long start) throws IOException	//Return the address of the parent of the node at pointer
	{		
		byte	numcells = 0;
		int		largestpage = 0, tmppage = 0, tmpkey = 0;
		
		file.seek(start);			//seek to the start of the page we are starting on
		
		if(file.readByte() == 0x0d)	//we were given a page that is a leaf to start
			return -1;
		
		numcells = file.readByte();
		file.readShort();
		largestpage = file.readInt();
		file.getFilePointer();
		
		for(int i = 0; i < numcells; i++)
		{
			file.seek(file.readShort() + start);	//go to the start of the list
			tmppage = file.readInt();				//store the page # of key <= to...
			tmpkey = file.readInt();				//this key
			if(key <= tmpkey)						//if the key we are looking for is less than or equal to the tmpkey (we found the page we want)
				{
					file.seek(tmppage*pageSize);	//seek to the page
					if(file.readByte() == 0x0d)		//if it is a leaf node it has to be the one we are looking for
						return start;
					else							//if it is an interior node then we need to keep looking on the interior node
						return FINDPARENT(file,key,tmppage*pageSize);
				}		
		}
		
		//if we make it here that means the key is > than the largest key stored in this interior node
		//so we have to check the node at the address of the largest key
		
		file.seek(largestpage*pageSize);			//go to that largest page
		if(file.readByte() == 0x0d)					//we found the leaf we were looking for
			return start;
		else										//we found another interior node
			return FINDPARENT(file,key,largestpage*pageSize);
	}
	
	public static int GETNUMCOLUMNS(String table) throws IOException
	{
		RandomAccessFile File = new RandomAccessFile("data\\greenwaldbase_columns.tbl","r");
				
		int pagepointer = 0, numcells, pointer, numcols = 0;
		
		String tmptable = "";
		
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
				File.readUTF();		//Skip the column_name
				File.readUTF();		//Get data_type
				File.readByte();	//Get the ordinal position
												
				if(tmptable.equals(table))				//If its the correct table
				{
					numcols++;
				}

				File.seek(arraypointer + 2);	//find next address on list
				arraypointer = File.getFilePointer();
				pointer = (int) ((pagepointer*pageSize) + File.readShort());				//get address
				
			}
			
			File.seek(pagepointer * pageSize - 1 + pageSize);		//Find next page in the line
			
			pagepointer = File.readByte();				//store that page
							
		}
		while (pagepointer != 0);
		
		return numcols;
	}
	
	public static void UPDATE(String usrCom)
	{
		try{
			
		String 	table, where, change, tmp;
		byte	numcells, tmpByte = 0;
		byte[] 	types;
		int 	rowid, ord, tmpint = -1, numcols;
		long	pagepointer = 0, arraypointer = 0, typepointer = 0, nullLong = 0;
		short	pointer = 0;
		
		table = usrCom.substring(6, usrCom.toUpperCase().indexOf("SET")).trim();
		
		where = usrCom.substring(usrCom.toUpperCase().indexOf("SET") + 4, usrCom.indexOf("=", usrCom.toUpperCase().indexOf("=" , usrCom.toUpperCase().indexOf("SET") + 4))).trim();
		
		tmp = usrCom.substring(usrCom.toUpperCase().indexOf("ROWID")+ 6).trim();
		
		tmp = tmp.replace("=", "").replace(";", "").trim();
		
		change = usrCom.substring(usrCom.indexOf("=") + 1, usrCom.toUpperCase().indexOf("WHERE") - 1).replace("\"", "").trim();
		
		rowid = Integer.parseInt(tmp);
				
		if (rowid <= 0)
		{
			System.out.println("rowid must be > 0...");
			return;
		}
		
		if(change.toUpperCase().contains("NULL"))
		{
			change = change.toUpperCase();
			
			if(!ISNULLABLE(table,where))
			{
				System.out.println("This value is NOT nullable.");
				return;
			}
		}
		
		
		RandomAccessFile F = new RandomAccessFile("data\\" + table + ".tbl", "rw");
		
		ord = GETORDINALITY(table, where);
		
		pagepointer = FINDKEYPAGE(F, rowid, 0);
		
		F.seek(pagepointer);
		F.readByte();
		numcells = F.readByte();
		F.readShort();
		arraypointer = F.getFilePointer();
		
		for(int i = 0; i < numcells && tmpint != rowid;i++)
		{
			pointer = F.readShort();
			F.seek(pointer + pageSize * pagepointer);
			F.readShort();
			tmpint = F.readInt();
			arraypointer += 2;
			if(tmpint != rowid)
				F.seek(arraypointer);
		}
		
		if(tmpint != rowid && rowid > 0)
			System.out.println("ERROR: Could not find rowid = " + rowid);
		else
		{
			numcols = F.readByte();
			
			types = new byte[numcols];
			
			for(int i = 0; i < numcols; i++)
			{
				if(ord == i+2)							//if the type we are about to read is the ord we want to update
					typepointer = F.getFilePointer();	//save that address for later
				types[i] = F.readByte();				//get the type
			}
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'_'HH:mm:ss");
			SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
			Date d;
			
			for(int j = 0; j < ord - 1; j++)
			{
				switch(types[j])
				{
				case 0:		//1 byte Null
					if(ord == j+2 && change.toUpperCase().equals("NULL"))			//if we find the ord and we are changing null to null
					{
						F.readByte();												//Skip the byte
						System.out.println("Changed a Null to a Null");				//Say we didnt change anything
					}else if(ord == j+2 && !change.toUpperCase().equals("NULL"))	//If we are at the ord and changing null to tinyint
					{
						nullLong = F.getFilePointer();								//save the pointer
						F.seek(typepointer);										//seek back to the type
						F.writeByte(0x04);											//change type to tinyint
						F.seek(nullLong);											//go back to pointer where tinyint is stored
						F.writeByte(Byte.parseByte(change));						//write the tinyint
						System.out.println("Changed a Null to : " + change);		//say we changed the null to a tinyint
					} else
						F.readByte();												//we are not at the right ordinal so keep going
					break;
				case 1:		//2 byte Null
					if(ord == j+2 && change.toUpperCase().equals("NULL"))
					{
						F.readShort();
						System.out.println("Changed a Null to a Null");
					}else if(ord == j+2 && !change.toUpperCase().equals("NULL"))
					{
						nullLong = F.getFilePointer();
						F.seek(typepointer);
						F.writeByte(0x05);
						F.seek(nullLong);
						F.writeShort(Short.parseShort(change));
						System.out.println("Changed a Null to : " + change);
					} else
						F.readShort();
					break;
				case 2:		//4 byte Null
					if(ord == j+2 && change.toUpperCase().equals("NULL"))			//changing a null to a null
					{
						F.readInt();
						System.out.println("Changed a Null to a Null");
					}else if(ord == j+2 && !change.toUpperCase().equals("NULL"))
					{
						nullLong = F.getFilePointer();								//save the location
						F.seek(typepointer);										//go to the type storage
						tmpByte = GETTYPEBYTE(GETDATATYPE(table,ord));				//Find what type we are updating to
						F.writeByte(tmpByte);										//write that type to the type storage
						F.seek(nullLong);											//go back to the info address
						if(tmpByte == 0x06)											//if it is an INT
							F.writeInt(Integer.parseInt(change));					//write it as an INT
						else
							F.writeFloat(Float.parseFloat(change));					//if not an INT it has to be a FLOAT
						System.out.println("Changed a Null to : " + change);
					} else
						F.readInt();
					break;
				case 3:		//8 byte Null
					if(ord == j+2 && change.toUpperCase().equals("NULL"))			//changing a null to a null
					{
						F.readLong();
						System.out.println("Changed a Null to a Null");
					}else if(ord == j+2 && !change.toUpperCase().equals("NULL"))
					{
						nullLong = F.getFilePointer();								//save the location
						F.seek(typepointer);										//go to the type storage
						tmpByte = GETTYPEBYTE(GETDATATYPE(table,ord));				//Find what type we are updating to
						F.writeByte(tmpByte);										//write that type to the type storage
						F.seek(nullLong);											//go back to the info address
						if(tmpByte == 0x07)											//if it is a BIGINT
							F.writeLong(Long.parseLong(change));					//write it as an BIGINT
						else if (tmpByte == 0x09)									//if it is a double
							F.writeDouble(Double.parseDouble(change));				//write it as a double
						else if (tmpByte == 0x0A)									//if it is a DATETIME
						{
							d = new Date();											//Clear up the date
							d = sdf.parse(where);									//parse the string into the date
							F.writeLong(d.getTime());								//write the long from the date
						} else														//if not BIGINT,DOUBLE, or DATETIME it has to be a DATE
						{
							d = new Date();											//Clear up the date
							d = sdf2.parse(where);									//parse the string into the date
							F.writeLong(d.getTime());								//write the long from the date
						}
						System.out.println("Changed a Null to : " + change);
					} else
						F.readLong();
				case 4:
					if(ord == j+2 && !change.toUpperCase().equals("NULL"))			//if we find the ord and we are changing not null to not null
					{
						F.writeByte(Byte.parseByte(change));
						System.out.println("Changed a TINYINT to: " + change);				
					}else if(ord == j+2 && change.toUpperCase().equals("NULL"))		//If we are at the ord and changing tinyint to a null
					{
						nullLong = F.getFilePointer();								//save the pointer
						F.seek(typepointer);										//seek back to the type
						F.writeByte(0x00);											//change type to null
						F.seek(nullLong);											//go back to pointer where tinyint is stored
						F.writeByte(0x00);											//overwrite the old tinyint
						System.out.println("Changed a TINYINT to : " + change);		//say we changed the null to a tinyint
					} else
						F.readByte();												//we are not at the right ordinal so keep going
					break;
				case 5:
					if(ord == j+2 && !change.toUpperCase().equals("NULL"))			//if we find the ord and we are changing not null to not null
					{
						F.writeShort(Short.parseShort(change));
						System.out.println("Changed a SMALLINT to: " + change);				
					}else if(ord == j+2 && change.toUpperCase().equals("NULL"))		//If we are at the ord and changing SMALLINT to a null
					{
						nullLong = F.getFilePointer();								//save the pointer
						F.seek(typepointer);										//seek back to the type
						F.writeByte(0x01);											//change type to null
						F.seek(nullLong);											//go back to pointer where smallint is stored
						F.writeShort(0x00);											//overwrite the old smallint
						System.out.println("Changed a SMALLINT to : " + change);	//say we changed the smallint to a null
					} else
						F.readShort();												//we are not at the right ordinal so keep going
					break;
				case 6:
					if(ord == j+2 && !change.toUpperCase().equals("NULL"))			//if we find the ord and we are changing not null to not null
					{
						F.writeInt(Integer.parseInt(change));
						System.out.println("Changed an INT to: " + change);				
					}else if(ord == j+2 && change.toUpperCase().equals("NULL"))		//If we are at the ord and changing INT to a null
					{
						nullLong = F.getFilePointer();								//save the pointer
						F.seek(typepointer);										//seek back to the type
						F.writeByte(0x02);											//change type to null
						F.seek(nullLong);											//go back to pointer where int is stored
						F.writeInt(0x00);											//overwrite the old int
						System.out.println("Changed an INT to : " + change);		//say we changed the int to a null
					} else
						F.readInt();												//we are not at the right ordinal so keep going
					break;
				case 7:
					if(ord == j+2 && !change.toUpperCase().equals("NULL"))			//if we find the ord and we are changing not null to not null
					{
						F.writeLong(Long.parseLong(change));
						System.out.println("Changed a BIGINT to: " + change);				
					}else if(ord == j+2 && change.toUpperCase().equals("NULL"))		//If we are at the ord and changing BIGINT to a null
					{
						nullLong = F.getFilePointer();								//save the pointer
						F.seek(typepointer);										//seek back to the type
						F.writeByte(0x03);											//change type to null
						F.seek(nullLong);											//go back to pointer where long is stored
						F.writeLong(0x00);											//overwrite the old long
						System.out.println("Changed a BIGINT to : " + change);		//say we changed the long to a null
					} else
						F.readLong();												//we are not at the right ordinal so keep going
					break;
				case 8:
					if(ord == j+2 && !change.toUpperCase().equals("NULL"))			//if we find the ord and we are changing not null to not null
					{
						F.writeFloat(Float.parseFloat(change));
						System.out.println("Changed a REAL to: " + change);				
					}else if(ord == j+2 && change.toUpperCase().equals("NULL"))		//If we are at the ord and changing Float to a null
					{
						nullLong = F.getFilePointer();								//save the pointer
						F.seek(typepointer);										//seek back to the type
						F.writeByte(0x02);											//change type to null
						F.seek(nullLong);											//go back to pointer where float is stored
						F.writeFloat(0x00);											//overwrite the old float
						System.out.println("Changed a REAL to : " + change);		//say we changed the float to a null
					} else
						F.readFloat();												//we are not at the right ordinal so keep going
					break;
				case 9:
					if(ord == j+2 && !change.toUpperCase().equals("NULL"))			//if we find the ord and we are changing not null to not null
					{
						F.writeDouble(Double.parseDouble(change));
						System.out.println("Changed a DOUBLE to: " + change);				
					}else if(ord == j+2 && change.toUpperCase().equals("NULL"))		//If we are at the ord and changing DOUBLE to a null
					{
						nullLong = F.getFilePointer();								//save the pointer
						F.seek(typepointer);										//seek back to the type
						F.writeByte(0x03);											//change type to null
						F.seek(nullLong);											//go back to pointer where DOUBLE is stored
						F.writeDouble(0x00);										//overwrite the old DOUBLE
						System.out.println("Changed a DOUBLE to : " + change);		//say we changed the DOUBLE to a null
					} else
						F.readDouble();												//we are not at the right ordinal so keep going
					break;
				case 10:
					if(ord == j+2 && !change.toUpperCase().equals("NULL"))			//if we find the ord and we are changing not null to not null
					{
						d = new Date();												//Clear up the date
						d = sdf.parse(where);										//parse the string into the date
						F.writeLong(d.getTime());									//write the long from the date
						System.out.println("Changed a DATETIME to: " + change);
					}else if(ord == j+2 && change.toUpperCase().equals("NULL"))		//If we are at the ord and changing DATETIME to a null
					{
						nullLong = F.getFilePointer();								//save the pointer
						F.seek(typepointer);										//seek back to the type
						F.writeByte(0x03);											//change type to null
						F.seek(nullLong);											//go back to pointer where DOUBLE is stored
						F.writeLong(0x00);											//overwrite the old DOUBLE
						System.out.println("Changed a DATETIME to : " + change);		//say we changed the DOUBLE to a null
					}else
						F.readLong();
					break;
				case 11:
					if(ord == j+2 && !change.toUpperCase().equals("NULL"))			//if we find the ord and we are changing not null to not null
					{
						d = new Date();												//Clear up the date
						d = sdf2.parse(where);										//parse the string into the date
						F.writeLong(d.getTime());									//write the long from the date
						System.out.println("Changed a DATE to: " + change);
					}else if(ord == j+2 && change.toUpperCase().equals("NULL"))		//If we are at the ord and changing DATETIME to a null
					{
						nullLong = F.getFilePointer();								//save the pointer
						F.seek(typepointer);										//seek back to the type
						F.writeByte(0x03);											//change type to null
						F.seek(nullLong);											//go back to pointer where DOUBLE is stored
						F.writeLong(0x00);											//overwrite the old DOUBLE
						System.out.println("Changed a DATE to : " + change);		//say we changed the DOUBLE to a null
					}else
						F.readLong();
					break;
				default:
					if(ord == j +2)													//if we find the ord
					{
						F.writeUTF(change);
						System.out.println("Changed a TEXT to: " + change);
					}
					else
						F.readUTF();
					break;
					
				}
			}
			
			F.close();
		}
		}
		catch(Exception e7)
		{
			System.out.println("Got exception in UPDATE");
			e7.printStackTrace();
		}
	}
	
	public static byte GETTYPEBYTE(String str)	//takes a string and converts it to the byte needed to be written in a record
	{
		byte B = -1;
		
		str = str.toUpperCase();
		switch(str)
		{
			case "INT":
				B = 0x06;
				break;
			case "BIGINT":
				B = 0x07;
				break;
			case "REAL":
				B = 0x08;
				break;
			case "DOUBLE":
				B = 0x09;
				break;
			case "DATETIME":
				B = 0x0A;
				break;
			case "DATE":
				B = 0x0B;
				break;
		}
		
		return B;
	}
	
	public static void SELECT_FROM_WHERE(String usrCom)
	{
		{			
			usrCom = usrCom.replace(";", "");
						
			String[] select;
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
					
					int numcells = 0;
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
		//System.out.println("Need to write DELETE_FROM_WHERE");
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
		int pagepointer = 0, numcells, pointer;
		
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
	
	public static boolean ISNULLABLE(String table, String column) throws IOException
	{
		RandomAccessFile File = new RandomAccessFile("data\\greenwaldbase_columns.tbl", "r");
		
		//byte tmpByte = 0;
		int pagepointer = 0, numcells, pointer;
		
		String tmptable = "", tmpIs = "", tmpColname = "";
		
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
				tmpIs = "";
				tmpColname = "";
				
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
				tmpColname = File.readUTF();//Get the column_name
				File.readUTF();				//Skip data_type
				File.readByte();			//Skip the ordinal position
				tmpIs = File.readUTF();		//Get is_nullable
												
				if(tmptable.equals(table) && tmpColname.equals(column) && tmpIs.equals("YES"))
				{
					File.close();
					return true;
				}
				else if (tmptable.equals(table) && tmpColname.equals(column) && tmpIs.equals("NO"))
				{
					File.close();
					return false;
				}

				File.seek(arraypointer + 2);	//find next address on list
				arraypointer = File.getFilePointer();
				pointer = (int) ((pagepointer*pageSize) + File.readShort());				//get address
				
			}
			
			File.seek(pagepointer * pageSize - 1 + pageSize);		//Find next page in the line
			
			pagepointer = File.readByte();				//store that page
							
		}
		while (pagepointer != 0);
		
		return false;
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
		
		String tmpstring = "";
		
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
		
		
		File.seek(pageSize*pointer2 +(pageSize-1));
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
	
	public static boolean KEYEXISTS(String table, int key) throws IOException
	{
		RandomAccessFile File = new RandomAccessFile("data\\" + table + ".tbl","r");
		
		int pagepointer = 0, numcells, pointer;
				
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
				File.seek(pointer);

				File.readShort();	//Skip over payload
				if(File.readInt() == key)		//Skip over rowid
				{
					File.close();
					return true;
				}
												
				

				File.seek(arraypointer + 2);	//find next address on list
				arraypointer = File.getFilePointer();
				pointer = (int) ((pagepointer*pageSize) + File.readShort());				//get address
				
			}
			
			File.seek(pagepointer * pageSize - 1 + pageSize);		//Find next page in the line
			
			pagepointer = File.readByte();				//store that page
							
		}
		while (pagepointer != 0);
		
		File.close();
		return false;
	}
	
	public static boolean ISNULLABLE(String table, int ord) throws IOException		//table and ord of column returns true is nullable false if not
	{
		RandomAccessFile File = new RandomAccessFile("data\\greenwaldbase_columns.tbl", "r");
		
		byte tmpByte = 0;
		int pagepointer = 0, numcells, pointer;
		
		String tmptable = "", tmpIs = "";
		
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
				tmpIs = "";
				
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
				File.readUTF();				//Get the column_name
				File.readUTF();				//Skip data_type
				tmpByte = File.readByte();	//Skip the ordinal position
				tmpIs = File.readUTF();		//Get is_nullable
												
				if(tmptable.equals(table) && tmpByte == ord && tmpIs.equals("YES"))
				{
					File.close();
					return true;
				}
				else if (tmptable.equals(table) && tmpByte == ord && tmpIs.equals("NO"))
				{
					File.close();
					return false;
				}

				File.seek(arraypointer + 2);	//find next address on list
				arraypointer = File.getFilePointer();
				pointer = (int) ((pagepointer*pageSize) + File.readShort());				//get address
				
			}
			
			File.seek(pagepointer * pageSize - 1 + pageSize);		//Find next page in the line
			
			pagepointer = File.readByte();				//store that page
							
		}
		while (pagepointer != 0);
		
		File.close();		
		return false;
	}
	
}






