package primary;

import java.io.IOException;
import java.io.RandomAccessFile;

public class Testing3 {
	
	public static int pageSize = 512;

	public static void main(String[] args) {
		
		byte i = 17;
		
		System.out.println(i/2);
		System.out.println((byte) Math.ceil((float)i/2));
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

}
