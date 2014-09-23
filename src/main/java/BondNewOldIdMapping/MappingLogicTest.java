package BondNewOldIdMapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.morningstar.FundAutoTest.commons.Helper;

public class MappingLogicTest {
	private static String MappingFilePath = "C:/HJG_WORK/HJG_Project/BondNewOldIdMapping/IdMapping/IdMapping.txt";
	Logger log = Logger.getLogger(MappingLogicTest.class);

	private List<String> loadMappingContent(String testingFilePath) {
		long startTime = System.currentTimeMillis();
		List<String> list = new ArrayList<String>();
		try {
			list = Helper.readFileList(testingFilePath);
		} catch (IOException e) {
			log.error(e.getMessage());
		}
		long endTime = System.currentTimeMillis() - startTime;
		if (!list.isEmpty()) {
			log.info("ResearchIdMapping content has been loaded,total cost: "
					+ endTime + " ms");
		}
		return list;
	}

// 测试生成的Mapping文档中无CUSIP也无ISIN的情况
	private void testMappingLogicNoCUSIPAndISIN(List<String> list) {
		log.info("Begin to test no CUSIP && ISIN case!");
		long startTime = System.currentTimeMillis();
		String oldCUSIP;
		String oldISIN;
		String[] ele;
		int lineNum=0;
		for (String content : list) {
			lineNum++;
			ele = content.split("\t",4);
			oldCUSIP = ele[0];
			oldISIN = ele[1];
			if (oldCUSIP != null && oldISIN != null) {
				if(oldCUSIP.isEmpty() && oldISIN.isEmpty()){
					log.error("No CUSIP && ISIN found in line number: "+lineNum);
				}
			}
		}
		long endTime = System.currentTimeMillis() - startTime;
		log.info("No CUSIP && ISIN test case has finished!Total cost:" +endTime+" ms");
	}
	
//测试生成的Mapping文档中无OldBondId也无NewInvestmentId的情况	
	private void testMappingLogicNoOldBondIdAndNoNewInvestmentId(List<String> list){
		log.info("Begin to test no OldBondId && NewInvestmentId case!");
		long startTime = System.currentTimeMillis();
		String oldBondId;
		String newInvestmentId;
		String[] ele;
		int lineNum=0;
		for (String content : list) {
			lineNum++;
			ele = content.split("\t",4);
			oldBondId = ele[2];
			newInvestmentId = ele[3];
			if (oldBondId != null && newInvestmentId != null) {
				if(oldBondId.isEmpty() && newInvestmentId.isEmpty()){
					log.error("No OldBondId && NewInvestmentId found in line number: "+lineNum);
				}
			}
		}
		long endTime = System.currentTimeMillis() - startTime;
		log.info("No OldBondId && NewInvestmentId test case has finished!Total cost:" +endTime+" ms");
	}
	
//测试出现只有NewInvestmentId却没有OldBondId的情况
	private void testMappingLogicNoOldBondIdButNewInvestmentId(List<String> list){
		log.info("Begin to test no OldBondId but got NewInvestmentId case!");
		long startTime = System.currentTimeMillis();
		String oldBondId;
		String newInvestmentId;
		String[] ele;
		int lineNum=0;
		for (String content : list) {
			lineNum++;
			ele = content.split("\t",4);
			oldBondId = ele[2];
			newInvestmentId = ele[3];
			if (oldBondId != null && newInvestmentId != null) {
				if(oldBondId.isEmpty() && !newInvestmentId.isEmpty()){
					log.error("No OldBondId but got NewInvestmentId case found in line number: "+lineNum);
				}
			}
		}
		long endTime = System.currentTimeMillis() - startTime;
		log.info("No OldBondId but got NewInvestmentId test case has finished!Total cost:" +endTime+" ms");
	}

// 测试生成的MAPPING文件不包括重复的CUSIP(补充：若有重复CUSIP时，只有一个应该有对应的NewInvestmentId,其他不带NewInvestmentId)
	private void testMappingLogicNoDuplicateCUSIP(List<String> list) {
		log.info("Begin to test no duplicate CUSIP case!");
		int count = 1;
		int duplicateCount = 1;
		String oldCUSIP;
		String newInvestmentId;
		String duplicateCUSIP;
		String[] ele;
		List<String> CUSIPList = new ArrayList<String>();
		Map<String, Integer> duplicateCUSIPMap = new HashMap<String, Integer>();
		List<String> newInvestmentIdCounter = new ArrayList<String>();
		long startTime = System.currentTimeMillis();
//building CUSIP list
		long startBuildCUSIPTime = System.currentTimeMillis();
		for(String content:list){
			ele = content.split("\t",4);
			oldCUSIP = ele[0];
			if(oldCUSIP != null){
				CUSIPList.add(oldCUSIP);
			}
		}
		long endBuildCUSIPTime = System.currentTimeMillis() - startBuildCUSIPTime;
		if(!CUSIPList.isEmpty()){
			log.info("CUSIP list has built up!Total cost: "+endBuildCUSIPTime+" ms");
		}
//Find duplicate CUSIP map		
		for(String cusip:CUSIPList){
			if(cusip != null && !cusip.isEmpty())
			if(duplicateCUSIPMap.containsKey(cusip)){
				duplicateCUSIPMap.put(cusip, duplicateCUSIPMap.get(cusip).intValue()+1);
			}else{
				duplicateCUSIPMap.put(cusip, count);
			}
		}
		for(Entry<String,Integer> entry:duplicateCUSIPMap.entrySet()){
			duplicateCount = entry.getValue();
			if(duplicateCount>1){
				duplicateCUSIP = entry.getKey();
				for(String lineContent:list){
					if(lineContent.contains(duplicateCUSIP)){
						ele = lineContent.split("\t",4);
						newInvestmentId = ele[3];
						if(!newInvestmentId.isEmpty()){
							newInvestmentIdCounter.add(newInvestmentId);
						}
					}
				}
				if(newInvestmentIdCounter.size()>1){
					log.error("Already find out duplicate CUSIP and remaining CUSIP also output the NewInvestmentId,the CUSIP is:"+entry.getKey()+" and duplicate count is: "+entry.getValue());
					newInvestmentIdCounter.clear();
				}else{
					newInvestmentIdCounter.clear();
				}
			}
		}
		long endTime = System.currentTimeMillis() - startTime;
		log.info("Duplicate CUSIP test case has finished!Total cost:" +endTime+" ms");
	}
	
//测试生成的MAPPING文件不包括重复的ISIN
	private void testMappingLogicNoDuplicateISIN(List<String> list){
		log.info("Begin to test no duplicate ISIN case!");
		int count = 1;
		int duplicateCount = 1;
		String oldISIN;
		String duplicateISIN;
		String newInvestmentId;
		String[] ele;
		List<String> ISINList = new ArrayList<String>();
		List<String> newInvestmentIdCounter = new ArrayList<String>();
		Map<String, Integer> duplicateISINMap = new HashMap<String, Integer>();
		long startTime = System.currentTimeMillis();
//building ISIN list
		long startBuildISINTime = System.currentTimeMillis();
		for(String content:list){
			ele = content.split("\t",4);
			oldISIN = ele[1];
			if(oldISIN != null){
				ISINList.add(oldISIN);
			}
		}
		long endBuildISINTime = System.currentTimeMillis() - startBuildISINTime;
		if(!ISINList.isEmpty()){
			log.info("ISIN list has built up!Total cost: "+endBuildISINTime+" ms");
		}
//Find duplicate ISIN map		
		for(String ISIN:ISINList){
			if(ISIN != null && !ISIN.isEmpty())
			if(duplicateISINMap.containsKey(ISIN)){
				duplicateISINMap.put(ISIN, duplicateISINMap.get(ISIN).intValue()+1);
			}else{
				duplicateISINMap.put(ISIN, count);
			}
		}
		for(Entry<String,Integer> entry:duplicateISINMap.entrySet()){
			duplicateCount = entry.getValue();
			if(duplicateCount>1){
				duplicateISIN = entry.getKey();
				for(String lineContent:list){
					if(lineContent.contains(duplicateISIN)){
						ele = lineContent.split("\t",4);
						newInvestmentId = ele[3];
						if(!newInvestmentId.isEmpty()){
							newInvestmentIdCounter.add(newInvestmentId);
						}
					}
				}
				if(newInvestmentIdCounter.size()>1){
					log.error("Already find out duplicate ISIN and remaining ISIN also output the NewInvestmentId,the ISIN is:"+entry.getKey()+" and duplicate count is: "+entry.getValue());
					newInvestmentIdCounter.clear();
				}else{
					newInvestmentIdCounter.clear();
				}				
			}
		}
		long endTime = System.currentTimeMillis() - startTime;
		log.info("Duplicate ISIN test case has finished!Total cost:" +endTime+" ms");
	}
	
//测试生成的MAPPING文件不包括完全重复的行
	private void testMappingLogicNoDuplicateLines(List<String> list){
		log.info("Begin to test no duplicate lines content case!");
		long startTime = System.currentTimeMillis();
		int count = 1;
		int duplicateCount = 1;
		Map<String, Integer> duplicateLineContentMap = new HashMap<String, Integer>();
//Find duplicate lines map		
		for(String lineContent:list){
			if(duplicateLineContentMap.containsKey(lineContent)){
				duplicateLineContentMap.put(lineContent, duplicateLineContentMap.get(lineContent).intValue()+1);
			}else{
				duplicateLineContentMap.put(lineContent, count);
			}
		}
		for(Entry<String,Integer> entry:duplicateLineContentMap.entrySet()){
			duplicateCount = entry.getValue();
			if(duplicateCount>1){
				log.error("Already catch duplicate lines,the line content is: "+entry.getKey()+" and duplicate count is: "+entry.getValue());
			}
		}
		long endTime = System.currentTimeMillis() - startTime;
		log.info("Duplicate lines test case has finished!Total cost:" +endTime+" ms");
	}
	
//测试生成的Mapping文件中是否有出现OldBondId=NewInvestmentId的情况
	private void testMappingLogicOldBondIdEqualToNewInvestmentId(List<String> list){
		log.info("Begin to test OldBondId=NewInvestmentId case!");
		long startTime = System.currentTimeMillis();
		int lineNum=0;
		String oldBondId;
		String newInvestmentId;
		String[] ele;
		for(String lineContent:list){
			lineNum++;
			ele = lineContent.split("\t", 4);
			oldBondId = ele[2];
			newInvestmentId = ele[3];
			if(oldBondId !=null && newInvestmentId != null){
				if(oldBondId == newInvestmentId){
					log.error("Already catching the line with OldBondId==NewInvestmentId,line number is: "+lineNum);
				}
			}
		}
		long endTime = System.currentTimeMillis() - startTime;
		log.info("OldBondId equals with NewInvestmentId test case has finished!Total cost:" +endTime+" ms");
	}

	public static void main(String[] args) {
		List<String> contentList = new ArrayList<String>();
		MappingLogicTest TestCase = new MappingLogicTest();
		contentList = TestCase.loadMappingContent(MappingFilePath);
		TestCase.testMappingLogicNoCUSIPAndISIN(contentList);
		TestCase.testMappingLogicNoOldBondIdAndNoNewInvestmentId(contentList);
		TestCase.testMappingLogicNoOldBondIdButNewInvestmentId(contentList);
		TestCase.testMappingLogicNoDuplicateCUSIP(contentList);
		TestCase.testMappingLogicNoDuplicateISIN(contentList);
		TestCase.testMappingLogicNoDuplicateLines(contentList);
		TestCase.testMappingLogicOldBondIdEqualToNewInvestmentId(contentList);
	}
}
