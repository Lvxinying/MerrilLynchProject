package com.morningstar.FundAutoTest;

//CSV文件格式解析，本例用于从ids.csv文件中读取所有的ID值
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;

public class CsvParser {
	private static final String CSV_FILE = "config/ids.csv";
	public static HashMap<String, List<String>> map = new HashMap<String, List<String>>();

	public static HashMap<String, List<String>> getDataMap() throws IOException {
		@SuppressWarnings("resource")
		CSVReader reader = new CSVReader(new FileReader(CSV_FILE));
		String[] nextLine;
		while ((nextLine = reader.readNext()) != null) {
			List<String> list = new ArrayList<String>();
			list.add(nextLine[1]);
			list.add(nextLine[2]);
			map.put(nextLine[0], list);
		}
		
		return map;
	}
	
	public static List<String> getDataList(int i){
		try {
			@SuppressWarnings("resource")
			CSVReader reader = new CSVReader(new FileReader(CSV_FILE));
			String[] nextLine;
			List<String> list = new ArrayList<String>();
			while ((nextLine = reader.readNext()) != null) {
				list.add(nextLine[i]);
			}
			return list;
		} catch (Exception e) {
			// TODO: handle exception
		}
		return null;
		
	}
}
