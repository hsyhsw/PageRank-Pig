package kr.ac.kaist.adward.pagerankpig.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by adward on 6/3/14.
 */
public class LUTImpl {

	private static LUTImpl ourInstance;

	private Map<String, Integer> lut;

	public static LUTImpl getInstance(String lutPath) throws FileNotFoundException {
		if (ourInstance == null)
			ourInstance = new LUTImpl(lutPath);
		return ourInstance;
	}

	private LUTImpl(String lutPath) throws FileNotFoundException {
		lut = new HashMap<String, Integer>();

		File lutDir = new File(lutPath);
		File[] files = lutDir.listFiles();
		for (File f : files) {
			Scanner sc = new Scanner(f);
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				String[] fields = line.split("\t"); // id, title
				if (fields.length == 2)
					lut.put(fields[1], Integer.parseInt(fields[0]));
			}
		}
	}

	public Integer lookup(String key) {
		return lut.get(key);
	}
}
