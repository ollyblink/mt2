package mapreduce.utils;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Some handy file methods
 * 
 * @author Oliver
 *
 */
public enum FileUtils {

	INSTANCE;

	public File createTmpFolder(String inputFilePath) {
		File folder = new File(inputFilePath + "/tmp/");
		if (folder.exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(folder);
		}
		folder.mkdirs();
		return folder;
	}

	public File createTmpFolder(String inputFilePath, String tmpFolderName) {
		File folder = new File(inputFilePath + "/" + tmpFolderName + "/");
		if (folder.exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(folder);
		}
		folder.mkdirs();
		return folder;
	}

	public void deleteTmpFolder(File folder) {
		String[] entries = folder.list();
		for (String s : entries) {
			File currentFile = new File(folder.getPath(), s);
			currentFile.delete();
		}
		folder.delete();
	}

	public void getFiles(File f, List<String> pathVisitor) {

		if (f.isFile())
			pathVisitor.add(f.getAbsolutePath());
		else {
			File files[] = f.listFiles();
			if (files != null) {
				for (int i = 0; i < files.length; i++) {
					getFiles(files[i], pathVisitor);
				}
			}
		}
	}

	public String readLines(String filePath) {
		String linesAsLine = "";
		ArrayList<String> lines = readLinesFromFile(filePath);
		for (String line : lines) {
			linesAsLine += line + "\n";
		}
		return linesAsLine;
	}

	public ArrayList<String> readLinesFromFile(String filePath) {
		ArrayList<String> lines = new ArrayList<String>();
		try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), Charset.forName("UTF-8"))) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
		} catch (IOException x) {
			System.err.format("IOException: %s%n", x);
		}
		return lines;
	}

	public void deleteFilesAndFolder(String outFolder, List<String> pathVisitor) {
		for (String fP : pathVisitor) {
			File file = new File(fP);
			if (file.exists()) {
//				System.err.println("deleting: "+ fP);
				file.delete();
			}
		}
		File file = new File(outFolder);
		if (file.exists()) {
//			System.err.println("deleting: "+ outFolder);
			file.delete();
		}
	}

}