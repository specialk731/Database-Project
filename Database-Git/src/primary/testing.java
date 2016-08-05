package primary;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class testing {
	
	public static int tables_rowid = 3;
	public static int columns_rowid = 9;
	public static int pageSize = 512;

	public static void main(String[] args) {
		
		String usrCom = "INSERT INTO TABLE greenwaldbase_columns VALUES (" + columns_rowid + ", \"tablename\", \"columnname\", \"INT\", 3, \"YES\");";
		
		String table = usrCom.substring(18, usrCom.indexOf(" ", 18));
				
		File file = new File("data\\" + table + ".tbl");
		
		if(file.isFile())
		{
		String[] values = usrCom.split(",");
		
		String[] tmp = values[0].split("\\(", 2);
		
		values[0]=tmp[1];
		
		values[values.length - 1] = values[values.length-1].replace(");", "");
		
		for(int i = 0; i < values.length; i++)
		{
			values[i] = values[i].trim().replace("\"", "");
			if(values[i].toLowerCase().contains("null"))
				values[i] = "NULL";
		}
						
		int key = Integer.parseInt(values[0]);
		
		System.out.println(usrCom);
		System.out.println("Table: " + table);
		
		//String type = SELECT_FROM_WHERE_tostring("SELECT DATA_TYPE FROM greenwaldbase_tables WHERE table_name=\"" + table + "\";");
				
		try{
			
		short payload = 0;
		
		String[] datatypes = new String [values.length];
		byte[] databytes = new byte [values.length];
		
		for(int i = 0; i < values.length ;i++)		//determine the datatypes
		{
			datatypes[i] = GETDATATYPE(table,i+1);
			
			if(datatypes[i].equals("TEXT"))
				System.out.println(datatypes[i] + " : " + values[i] + " : " + values[i].length());
			else
				System.out.println(datatypes[i] + " : " + values[i]);
		}
		
		for(int i = 1; i < values.length; i++)	//determine the size of the payload
			{
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
			
		long pointer = 0, tmpPointer = 0, pagePointer = 0, parentPage = 0, previousPage = 0;
		int tempKey = 0, numcells = 0;
			
		RandomAccessFile f = new RandomAccessFile("data\\" + table + ".tbl", "rw");
		
		pagePointer = FINDKEYPAGE(f,key,0);		//Find the page the record should go on		
		f.seek(pagePointer);					//Seek to that page		
		f.readByte();							//throw away page type		
		numcells = f.readByte();				//get numcells
		pointer = f.readShort();				//get offset of start of content
		
		System.out.println(numcells);
		
		for(int i = 0; i < numcells; i++)
			f.readShort();
				
		if((pointer + pagePointer) - f.getFilePointer() < payload + 6)
			LEAFSPLIT(f,pagePointer);
		
		previousPage = FINDPREVIOUSPAGE(f, pagePointer);
		
		pagePointer = FINDKEYPAGE(f,key,0);
		
		f.close();
		
		}
		catch(Exception e2)
		{
			e2.printStackTrace();		}
		}
		else
			System.out.println("The table: \"" + table + "\" could not be found.");
		
	}
	
	public static void LEAFSPLIT(RandomAccessFile file, long pointer) throws IOException
	{
		long parentPointer = 0;
		
		parentPointer = FINDPARENT(file, pointer);
		System.out.println("In LEAFSPLIT");
	}
	
	public static long FINDPARENT(RandomAccessFile file, long pointer)
	{
		return 0;
	}
	
	public static long FINDPREVIOUSPAGE(RandomAccessFile file, long pointer) throws IOException
	{
		long pointer2 = 0, pointer3 = 0;
		
		pointer2 = FINDLEFTMOSTPAGE(file,0);
				
		file.seek(pointer2 + pageSize - 1);
		pointer3 = file.readByte() * pageSize;
		
		while(pointer3 != pointer && pointer3 != 0)
		{
			pointer2 = pointer3;
			file.seek(pointer2 + pageSize -1);
			pointer3 = file.readByte() * pageSize;
		}
		
		return pointer3;
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
			pointer = (pagepointer*pageSize) + File.readShort();		//get address of first record
					
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
				pointer = (pagepointer*pageSize) + File.readShort();				//get address
				
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
