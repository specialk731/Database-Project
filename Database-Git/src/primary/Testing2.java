package primary;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class Testing2 {

	public static void main(String[] args) {
		
		try{
			RandomAccessFile file = new RandomAccessFile("data\\greenwaldbase_columns.tbl","rw");
			
			int[] test = {1,6};
			DISPLAYRECORD(file,DISPLAYRECORD(file,0x33,test), test);
			
			file.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
		}

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

}
