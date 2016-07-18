package primary;

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
public class DavisBaseBinaryFileExample {
	static int pageSize = 512;
	
	public static void main(String[] args) {
		/* This method will initialize the database storage 
		 * If there are any */
		initializeDataStore();

		/* Create some user table. An actual NEW user table would begin with a single page.
		 * However, this example demonstrates how to add/remove increments of pages to/from files.
		 *
		 * Whenever the length of a RandomAccessFile is increased, the added space is padded
		 * with 0x00 value bytes.
		 */
		String fileName = "some_user_table.tbl";
		RandomAccessFile binaryFile;
		try {
			binaryFile = new RandomAccessFile("data/" + fileName, "rw");

			/* Initialize the file size to be zero */
			binaryFile.setLength(0);
			System.out.println("The file is now " + binaryFile.length() + " bytes long");
			System.out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
			System.out.println();
			
			/* Increase the file size to be 1024, i.e. 2 x 512B */
			binaryFile.setLength(pageSize * 5);
			System.out.println("The file is now " + binaryFile.length() + " bytes long");
			System.out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
			System.out.println();
			
			/* Increase the file size to be 1024, i.e. 2 x 512B */
			binaryFile.setLength(pageSize * 3);
			System.out.println("The file is now " + binaryFile.length() + " bytes long");
			System.out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
			System.out.println();

			/* Re-locate the address pointer at the beginning of page 1 and write 
			 * 0x05 (b-tree interior node) to the first header byte */
			binaryFile.seek(0);
			binaryFile.writeByte(0x05);

			/* Re-locate the address pointer at the beginning of page 2 and write 
			 * 0x0D (b-tree leaf node) to the first header byte */
			binaryFile.seek(pageSize * 1);
			binaryFile.writeByte(0x0D);

			/* Re-locate the address pointer at the beginning of page 3 and write 
			 * 0x0D (b-tree leaf node) to the first header byte */
			binaryFile.seek(pageSize * 2);
			binaryFile.writeByte(0x0D);

			/* Increase the size of the binaryFile by exactly one page, regardless of how
			 * long it currently is. The new bytes will be appended to the end and be all zeros */
			binaryFile.setLength(binaryFile.length() + pageSize);
			System.out.println("The file is now " + binaryFile.length() + " bytes long");
			System.out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
			System.out.println();
			
			displayBinaryHex(binaryFile);
			binaryFile.close();
			
		}
		catch (Exception e) {
			System.out.println("Unable to open " + fileName);
		}

	}
	


	/**
	 * This static method creates the DavisBase data storage container
	 * and then initializes two .tbl files to implement the two 
	 * system tables, davisbase_tables and davisbase_columns
	 *
	 *  WARNING! Calling this method will destroy the system database
	 *           catalog files if they already exist.
	 */
	static void initializeDataStore() {

		/** Create data directory at the current OS location to hold */
		try {
			File dataDir = new File("data");
			dataDir.mkdir();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				File anOldFile = new File(dataDir, oldTableFiles[i]); 
				anOldFile.delete();
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}

		/** Create davisbase_tables system catalog */
		try {
			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			/* Initially, the file is one page in length */
			davisbaseTablesCatalog.setLength(pageSize);
			/* Set file pointer to the beginnning of the file */
			davisbaseTablesCatalog.seek(0);
			/* Write 0x0D to the page header to indicate that it's a leaf page.  
			 * The file pointer will automatically increment to the next byte. */
			davisbaseTablesCatalog.write(0x0D);
			/* Write 0x00 (although its value is already 0x00) to indicate there 
			 * are no cells on this page */
			davisbaseTablesCatalog.write(0x00);
			davisbaseTablesCatalog.close();
		}
		catch (Exception e) {
			System.out.println("Unable to create the database_tables file");
			System.out.println(e);
		}

		/** Create davisbase_columns systems catalog */
		try {
			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			/** Initially the file is one page in length */
			davisbaseColumnsCatalog.setLength(pageSize);
			davisbaseColumnsCatalog.seek(0);       // Set file pointer to the beginnning of the file
			/* Write 0x0D to the page header to indicate a leaf page. The file 
			 * pointer will automatically increment to the next byte. */
			davisbaseColumnsCatalog.write(0x0D);
			/* Write 0x00 (although its value is already 0x00) to indicate there 
			 * are no cells on this page */
			davisbaseColumnsCatalog.write(0x00); 
			davisbaseColumnsCatalog.close();
		}
		catch (Exception e) {
			System.out.println("Unable to create the database_columns file");
			System.out.println(e);
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
			while(ram.getFilePointer() < size) {
				System.out.print(String.format("%02X ", ram.readByte()));
				// System.out.print(ram.readByte() + " ");
				if(row % 16 == 0) {
					System.out.println();
					System.out.print(String.format("%04d\t0x%04X\t", row, row));
				}
				row++;
			}		
		}
		catch (Exception e) {
			System.out.println(e);
		}
	}
}