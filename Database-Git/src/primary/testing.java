package primary;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class testing {
	
	public static int columns_rowid = 8;
	public static int pageSize = 512;

	public static void main(String[] args) {
		
		String usrCom = "SELECT rowid fROM greenwaldbase_columns;";
		
		usrCom = usrCom.replace(";", "");
		
		String usrComlower = usrCom.toLowerCase();
		
		System.out.println(usrCom);

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
		
		for(int i = 0; i < select.length; i++)
		{
			select[i]=select[i].trim();
			System.out.println("Select " + (i+1) + ": " + select[i]);
		}
		
		System.out.println(table);
		System.out.println(where);
				
		File f= new File("data\\" + table + ".tbl");
		
		if(f.isFile())
		{
			try
			{
				int[] ordinals = new int[select.length];
				
				for(int i = 0; i < select.length; i++)
					ordinals[i] = GETORDINALITY(table, select[i]);
				
				if(!contains(ordinals, -1))
				{
				
				RandomAccessFile File = new RandomAccessFile("data\\" + table + ".tbl","rw");
				
				int numcols;
				
				long leftmost;
				
				if(select[0].equals("*"))  //NEEDS WORK!!!!!
				{
					ordinals = new int [select.length];
					for(int i = 0; i < select.length; i++)
						ordinals[i] = i+1;
				}
				else
					numcols = select.length;
				
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
					
					if(operator.equals("<=") || operator.equals("<"))
					{
						
					}else if (operator.equals(">=") || operator.equals(">"))
					{
						
					}else 
					{
						
					}
					
					System.out.println(operator);
					System.out.println(compareto);
				}
				else if(!where.equals("NO WHERE")) //WHERE is not on rowid
				{
					String compcol = where.substring(0, where.indexOf("=")).trim();			
					
					System.out.println(compcol);

					where = where.substring(where.indexOf("=") + 1).trim();
					
					
					
					System.out.println(compcol);
					System.out.println("New where: " + where);
				}
				else //NO WHERE
				{
					byte pointer2 = 0;

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
					
					int numcells = File.readByte();
					File.readShort();
					long arraypointer = File.getFilePointer();
					int pointer = pageSize*pointer2 + File.readShort();					
					
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
				System.out.println(e4);
				e4.printStackTrace();
			}
		}
		else
			System.out.println("Error: The table \"" + table + "\" does not exist.");
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
				//System.out.println(pointer);

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

	
	/*public static long DISPLAYRECORD(RandomAccessFile file, long pointer, int numcols) throws IOException
	{
		String print;
				
		file.seek(pointer);
		short payload = file.readShort();
		int key = file.readInt();
		
		print = "|\t" + key + "\t";
		
		byte numcol_available = file.readByte();
		
		if(numcol_available + 1 < numcols)
		{
			System.out.println("ERROR: you are asking for to many columns.");
			IOException e = new IOException();
			throw e;
		}
		
		byte[] types = new byte[numcols];
		
		for(int i = 0; i < numcols; i++)
			types[i] = file.readByte();
		
		file.seek(pointer + 7 + numcol_available);
		
		long tmp;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'_'HH:mm:ss");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
		Date d;
		
		for(int j = 0; j < numcols - 1; j++)
		{
			switch(types[j])
			{
			case 0:		//1 byte Null
				file.readByte();
				print = print + "|\t" + "NULL" + "\t";
				break;
			case 1:		//2 byte Null
				file.readShort();
				print = print + "|\t" + "NULL" + "\t";
				break;
			case 2:		//4 byte Null
				file.readInt();
				print = print + "|\t" + "NULL" + "\t";
				break;
			case 3:		//8 byte Null
				file.readLong();
				print = print + "|\t" + "NULL" + "\t";
				break;
			case 4:
				print = print + "|\t" + file.readByte() + "\t";
				break;
			case 5:
				print = print + "|\t" + file.readShort() + "\t";
				break;
			case 6:
				print = print + "|\t" + file.readInt() + "\t";
				break;
			case 7:
				print = print + "|\t" + file.readLong() + "\t";
				break;
			case 8:
				print = print + "|\t" + file.readFloat() + "\t";
				break;
			case 9:
				print = print + "|\t" + file.readDouble() + "\t";
				break;
			case 10:
				tmp = file.readLong();
				d = new Date(TimeUnit.SECONDS.toMillis(tmp));				
				print = print + "|\t" + sdf.format(d) + "\t";
				break;
			case 11:
				tmp = file.readLong();
				d = new Date(TimeUnit.SECONDS.toMillis(tmp));				
				print = print + "|\t" + sdf2.format(d) + "\t";
				break;
			default:
				print = print + "|\t" + file.readUTF() + "\t";
				
			}
		}
		
		print = print + " |";
	
		System.out.println(print);
		
		pointer = pointer + 6 + payload;
		
		return pointer;
	}
	
	public static void INSERT_INTO_TABLE(String str)
	{
		System.out.println(str);
	}*/

}
