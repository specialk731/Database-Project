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
		
		String usrCom = "INSERT INTO TABLE greenwaldbase_tables (rowid, table_name) VALUEs (3, test);";
		
		try{
			
		String table = usrCom.substring(18, usrCom.indexOf(" ", 18));
				
		File file = new File("data\\" + table + ".tbl");
		
		usrCom = usrCom.replaceFirst("(?i)values(?-i)", "VALUES");
		
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
		
		System.out.println("usrCom: " + usrCom);
		System.out.println("Table: " + table);
		
		//String type = SELECT_FROM_WHERE_tostring("SELECT DATA_TYPE FROM greenwaldbase_tables WHERE table_name=\"" + table + "\";");
			
			for(int i = 0; i < values.length;i++)
				if(!ISNULLABLE(table,(i+1)) && values[i].equals("NULL"))
				{
					System.out.println("ERROR: NULL values in a non nullable column");
					return;
				}
		
		short payload = 1;	//Every record has 1 in payload for num cols
		
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
		
		System.out.println("Payload: " + payload);
		if(payload + 11 > pageSize)
		{
			System.out.println("This record is to large for the Page Size.");
			return;
		}
			
		long pointer = 0, pagePointer = 0, arraypointer = 0;
		int tempKey = 0, numcells = 0;
		short tmparray = 0, tmparray2 = 0, startPointer = 0, tmparraynext = 0;
			
		RandomAccessFile f = new RandomAccessFile("data\\" + table + ".tbl", "rw");
		
		pagePointer = FINDKEYPAGE(f,key,0);		//Find the page the record should go on		
		f.seek(pagePointer);					//Seek to that page		
		f.readByte();							//throw away page type		
		numcells = f.readByte();				//get numcells
		pointer = f.readShort();				//get offset of start of content
		
		System.out.println(numcells);
		
		for(int i = 0; i < numcells; i++)		//Skip to the start of free space after address array
			f.readShort();
				
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
			f.seek(pagePointer*pageSize + tmparray);			//seek to the page number times page size + offset of the smallest record
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
		
		f.seek((pagePointer*pageSize) + startPointer);
		f.writeShort(payload);
		f.writeInt(key);
		f.writeByte(values.length - 1);
		
		for(int i = 1; i < values.length; i++)
		{
			System.out.println(values[i]);
			System.out.println(datatypes[i]);
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
						f.writeUTF(values[i]);
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
	
	public static boolean ISNULLABLE(String table, int ord) throws IOException		//table and ord of column returns true is nullable false if not
	{
		RandomAccessFile File = new RandomAccessFile("data\\greenwaldbase_columns.tbl", "r");
		
		byte tmpByte = 0;
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
	
	public static boolean KEYEXISTS(String table, int key) throws IOException
	{
		RandomAccessFile File = new RandomAccessFile("data\\" + table + ".tbl","r");
		
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
	
	public static void LEAFSPLIT(RandomAccessFile file, long pointer) throws IOException	//Give file and pointer to leaf you want split	
	{																						//KEEP THE LIST CHAIN GOING HERE
		
	}
	
	public static long FINDPARENT(RandomAccessFile file, int key, long start) throws IOException	//Return the address of the parent of the node at pointer
	{		
		byte	numcells = 0;
		int		largestpage = 0, tmppage = 0, tmpkey = 0;
		long	arraypointer = 0;
		
		file.seek(start);			//seek to the start of the page we are starting on
		
		if(file.readByte() == 0x0d)	//we were given a page that is a leaf to start
			return -1;
		
		numcells = file.readByte();
		file.readShort();
		largestpage = file.readInt();
		arraypointer = file.getFilePointer();
		
		for(int i = 0; i < numcells; i++)
		{
			file.seek(arraypointer);				//go to the next array element
			file.seek(start + file.readShort());	//go to the smallest record
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
			arraypointer += 2; //					//increase arrapointer to point at the next element
		
		}
		
		//if we make it here that means the key is > than the largest key stored in this interior node
		//so we have to check the node at the address of the largest key
		
		file.seek(largestpage*pageSize);			//go to that largest page
		if(file.readByte() == 0x0d)					//we found the leaf we were looking for
			return start;
		else										//we found another interior node
			return FINDPARENT(file,key,largestpage*pageSize);
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
