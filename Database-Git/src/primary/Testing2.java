package primary;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Testing2 {
	
	public static int pageSize = 512;
	public static int columns_rowid = 8;

	public static void main(String[] args) {
		try{
			
			RandomAccessFile File = new RandomAccessFile("data\\greenwaldbase_columns.tbl","rw");
		
			LEAFSPLIT(File, 0);
			LEAFSPLIT(File, 512);
			
			File.close();
		
		}catch(Exception e)
		{
			e.printStackTrace();
		}
			
	}
	
	public static void LEAFSPLIT(RandomAccessFile file, long page0) throws IOException	//Give file and pointer to leaf you want split	
	{																						//KEEP THE LIST CHAIN GOING HERE
		System.out.println("In LEAFSPLIT");

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
		/*System.out.println("Numcells: " + numcells);
		System.out.println("Numcells1: " + numcells1);
		System.out.println("Splitkey: " + splitkey);*/
		
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
		
		for(int i = 0; i < arraylist1.length; i++)
			System.out.println(arraylist1[i]);
		
		if(parentPointer == -1)		//This is the first split. We are on page 0
		{
			System.out.println("Splitting root as Leaf");
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
			file.writeShort(pageSize - 8);
			file.writeInt(2);
			file.writeShort(pageSize - 8);
			file.seek(pageSize - 8);
			file.writeInt(1);
			file.writeInt(splitkey);
			
		}
		else						//This is not the first split and we can proceed
		{
			System.out.println("Splitting Leaf");
			
			file.setLength(file.length() + pageSize);		//add another page
			page1 = file.length() - pageSize;
			System.out.println(page1);
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
			System.out.println(arraypointer2);
			
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
		System.out.println("parentpointer: " + parentpointer + " page0: " + page0 + " key: " + key + " page1: " + page1);
		
		boolean	done = false;
		byte	numcells = 0;
		short	offset = 0;
		int 	rightmost = 0, tmpkey1 = 0, tmppage1 = 0;
		
		file.seek(parentpointer);		//start at the parent
		file.readByte();				//skip page type
		numcells = file.readByte();		//get numcells
		offset = file.readShort();		//get offset
		
		if((10 + 8 * (numcells + 1) > pageSize))
				parentpointer = SPLITPARENT(file, parentpointer, key);		//needs to return the long pointer to the page the key needs to go on
		
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
		
		System.out.println("Filepointer: " + file.getFilePointer());		
		
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
	
	public static long SPLITPARENT(RandomAccessFile file, long parentpointer, int key) throws IOException //needs to return the long pointer to the page the key needs to go on
	{
		long parent1 = 0, parent2 = 0;
		
		if(parentpointer == 0) //splitting the root node
		{
			file.setLength(file.length() + pageSize*2);
			
		}
		
		file.seek(parentpointer);
		
		
		
		return 0;
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
		
		for(int i = 0; i < ordinals.length; i++)
			System.out.println(ordinals[i]);
		
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
		
		return false;
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
}