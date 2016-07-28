package primary.Examples;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;


/**
 *
 * @author Chris Irwin Davis
 * @version 1.0
 */
public class DavisBaseTableTreeExample {
	static int pageSize = 512;
	
	public static void main(String[] args) {

		String fileName = "city.tbl";
		RandomAccessFile binaryFile;
		try {
			binaryFile = new RandomAccessFile(fileName, "rw");

			/* Initialize the file size to be zero */
			binaryFile.setLength(0);            // Initialize the file size to be zero (in case it already exists)
			binaryFile.setLength(pageSize * 3); // Set the file size to be three pages in length


			/**
			 *  Define page 1
			 */
			short page1start = 0;
			/* Page 1 header and cell array */
			binaryFile.seek(page1start);        // Move the address pointer to the beginning of Page 1, i.e table tree root
			binaryFile.writeByte(0x05);         // The first page is a table interior page
			binaryFile.writeByte(0x01);         // Number of cells on this page
			binaryFile.writeShort(0x01F8);      // offset address of start of content area
			binaryFile.writeInt(3);             // A pointer to the rightmost child page (4-byte INT)
			/* The array of cell locations */
			binaryFile.write(0x01F8);			// offset address of first cell
			/* Page 1 cell contents */
			binaryFile.seek(0x01F8);
			binaryFile.writeInt(2);				// A pointer to the subtree < or = to the first key
			binaryFile.writeInt(3);				// The first key of the interior page/node


			/**
			 *  Define page 2
			 */
			short page2start = (short)(pageSize * 1);
			/* Page 2 header and cell array */
			binaryFile.seek(page2start);     // Move the address pointer to the beginning of the Page 2
			binaryFile.writeByte(0x0D);      // The first page is a table interior page
			binaryFile.writeByte(0x03);      // Number of cells on this page
			binaryFile.writeShort(0x01BD);   // offset address of start of content area
			/* The array of cell locations */
			binaryFile.writeShort(0x01E8);   // offset address of record rowid=1
			binaryFile.writeShort(0x01D2);   // offset address of record rowid=2
			binaryFile.writeShort(0x01BD);   // offset address of record rowid=3

			/* Record rowid=1 at offset 0x01E8 */
			binaryFile.seek(page2start + 0x1E8); // Offset address to begin writing record rowid=1
			binaryFile.writeShort(18);           // Size of payload
			binaryFile.writeInt(1);              // rowid=1 (is also column_1)
			binaryFile.writeByte(3);             // number of columns in addition to rowid column_1
			binaryFile.writeByte(5);             // column_2 data type byte SMALLINT 2-byte integer
			binaryFile.writeByte(8);             // column_3 data type byte REAL 4-byte single precision float
			binaryFile.writeByte(20);            // column_4 data type byte STRING 12B + 8 ASCII characters
			binaryFile.writeShort(7482);         // column_2 value
			binaryFile.writeFloat(1.5F);         // column_3 value
			binaryFile.writeBytes("New York");   // column_4 value

			/* Record rowid=2 at offset 0x01D2 */
			binaryFile.seek(page2start + 0x1D2); // Offset address to begin writing record rowid=1
			binaryFile.writeShort(15);           // Size of payload
			binaryFile.writeInt(2);              // rowid=2 (is also column_1)
			binaryFile.writeByte(3);             // number of columns in addition to rowid column_1
			binaryFile.writeByte(5);             // column_2 data type byte SMALLINT 2-byte integer
			binaryFile.writeByte(8);             // column_3 data type byte REAL 4-byte single precision float
			binaryFile.writeByte(18);            // column_4 data type byte STRING 12B + 8 ASCII characters
			binaryFile.writeShort(5211);         // column_2 value
			binaryFile.writeFloat(2.71F);        // column_3 value
			binaryFile.writeBytes("Berlin");     // column_4 value

			/* Record rowid=3 at offset 0x1BD */
			binaryFile.seek(page2start + 0x1BD); // Offset address to begin writing record rowid=1
			binaryFile.writeShort(14);           // Size of payload
			binaryFile.writeInt(3);              // rowid=3 (is also column_1)
			binaryFile.writeByte(3);             // number of columns in addition to rowid column_1
			binaryFile.writeByte(5);             // column_2 data type byte SMALLINT 2-byte integer
			binaryFile.writeByte(8);             // column_3 data type byte REAL 4-byte single precision float
			binaryFile.writeByte(17);            // column_4 data type byte STRING 12B + 8 ASCII characters
			binaryFile.writeShort(8295);         // column_2 value
			binaryFile.writeFloat(3.75F);        // column_3 value
			binaryFile.writeBytes("Paris");      // column_4 value


			/**
			 *  Define page 3
			 */
			short page3start = (short)(pageSize * 2);
			/* Page 3 header and cell array */
			binaryFile.seek(page3start);     // Move the address pointer to the beginning of the Page 2
			binaryFile.writeByte(0x0D);      // The first page is a table interior page
			binaryFile.writeByte(0x03);      // Number of cells on this page
			binaryFile.writeShort(0x01C0);   // offset address of start of content area
			/* The array of cell locations */
			binaryFile.writeShort(0x1EC);    // offset address of record rowid=1
			binaryFile.writeShort(0x01D6);   // offset address of record rowid=2
			binaryFile.writeShort(0x01C0);   // offset address of record rowid=3


			/* Record rowid=1 at offset 0x1E0 */
			binaryFile.seek(page3start + 0x1EC); // Offset address to begin writing record rowid=1
			binaryFile.writeShort(14);           // Size of payload
			binaryFile.writeInt(4);              // rowid=4 (is also column_1)
			binaryFile.writeByte(3);             // number of columns in addition to rowid column_1
			binaryFile.writeByte(5);             // column_2 data type byte SMALLINT 2-byte integer
			binaryFile.writeByte(8);             // column_3 data type byte REAL 4-byte single precision float
			binaryFile.writeByte(16);            // column_4 data type byte STRING 12B + 8 ASCII characters
			binaryFile.writeShort(10340);        // column_2 value
			binaryFile.writeFloat(0.33F);        // column_3 value
			binaryFile.writeBytes("Rome");       // column_4 value

			/* Record rowid=2 at offset 0x01C0 */
			binaryFile.seek(page3start + 0x1D6); // Offset address to begin writing record rowid=1
			binaryFile.writeShort(15);           // Size of payload
			binaryFile.writeInt(5);              // rowid=5 (is also column_1)
			binaryFile.writeByte(3);             // number of columns in addition to rowid column_1
			binaryFile.writeByte(5);             // column_2 data type byte SMALLINT 2-byte integer
			binaryFile.writeByte(8);             // column_3 data type byte REAL 4-byte single precision float
			binaryFile.writeByte(18);            // column_4 data type byte STRING 12B + 8 ASCII characters
			binaryFile.writeShort(3401);         // column_2 value
			binaryFile.writeFloat(3.8F);         // column_3 value
			binaryFile.writeBytes("Moscow");     // column_4 value

			/* Record rowid=3 at offset 0x01A0 */
			binaryFile.seek(page3start + 0x1C0); // Offset address to begin writing record rowid=1
			binaryFile.writeShort(15);           // Size of payload
			binaryFile.writeInt(6);              // rowid=6 (is also column_1)
			binaryFile.writeByte(3);             // number of columns in addition to rowid column_1
			binaryFile.writeByte(5);             // column_2 data type byte SMALLINT 2-byte integer
			binaryFile.writeByte(8);             // column_3 data type byte REAL 4-byte single precision float
			binaryFile.writeByte(18);            // column_4 data type byte STRING 12B + 8 ASCII characters
			binaryFile.writeShort(1217);         // column_2 value
			binaryFile.writeFloat(2.89F);        // column_3 value
			binaryFile.writeBytes("Madrid");     // column_4 value


			displayBinaryHex(binaryFile);
			binaryFile.close();
			
		}
		catch (Exception e) {
			System.out.println("Unable to open " + fileName);
		}

	}
	
	
	/**
	 * <p>This method is used for debugging.
	 * @param ram is an instance of {@link RandomAccessFile}. 
	 * <p>This method will display the binary contents of the file to Stanard Out (stdout)
	 */
	static void displayBinaryHex(RandomAccessFile ram) {
		try {
			System.out.println("Dec\tHex\t 0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F");
			ram.seek(0);
			long size = ram.length();
			int row = 1;
			System.out.print("0000\t0x0000\t");
			int rowCount = 1;
			while(ram.getFilePointer() < size) {
				System.out.print(String.format("%02X ", ram.readByte()));
				// System.out.print(ram.readByte() + " ");
				if(row % 16 == 0) {
					System.out.println();
					System.out.print(String.format("%04d\t0x%04X\t", row, row));
				}
				row++;
			}		
			if(row % 256 == 0) {
				System.out.println();
			}
		}
		catch (Exception e) {
			System.out.println(e);
		}
	}
}