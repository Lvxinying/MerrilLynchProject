package MarketPriceBackFeedToTSDB;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.data.tsapi.TSException;

public class NonTradeDatesBaseObject {
	private static String logPath = "./log/TestLog/MarketPriceBackFeed/";
	private static String logName1 = "MissingRecord_DBI_NonTradingDate(BaseObject).log";
	private static String logName2 = "ContentChecking_DBI_NonTradingDate(BaseObject).log";
	static SimpleDateFormat df1 = new SimpleDateFormat("yyyyMMdd");
	static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
	
//Get PerformanceIdList(EOD Mode)
	private static List<String> getUpdatedPerformanceIdListEODMode(String tableName,String createdOnTime) throws SQLException{
		List<String> list = new ArrayList<String>();
		long startTime = System.currentTimeMillis();
//保留CREATEDON的使用
		if(createdOnTime.isEmpty()){
			createdOnTime = "2000-01-01";
		}
		String sqlToGetUpdatedPerformanceId = "SELECT DISTINCT PERFORMANCEID FROM ADT_EXCHANGEUNIVERSE EU"+
								 " JOIN ADT_NOTIFICATIONDRIVER ND ON ND.ID = EU.EXCHANGEID AND ND.STARTDATE = EU.EFFECTIVEDATE AND ND.TABLENAME = EU.TABLENAME"+
								 " WHERE ND.IDTYPE = 2 AND ND.TABLENAME = '" + tableName + "' AND ND.CREATEDON >= '" + createdOnTime + "'";
		list = DBCommons.getDataList(sqlToGetUpdatedPerformanceId, Database.Netezza1);
		long endTime = System.currentTimeMillis() - startTime;
		System.out.println("[INFO]Got "+ list.size()+" performanceids(EOD MODE),time cost: "+endTime+" ms");
		return list;
	}

//Get PerformanceIdList(Individual Mode)
	private static List<String> getUpdatedPerformanceIdListIndividualMode(String testedTableName,int TsType, int testedIdCounts){
		List<String> list = new ArrayList<String>();
		long startTime = System.currentTimeMillis();
//选取IdType != 2的所有情况，且PROCESSNAME为DMRI for Equity或者DMRI for ETF才能保证是BASE OBJECT的		
		String sqlToGetUpdatedPerformanceIdList = "SELECT ID FROM ADT_NOTIFICATIONDRIVER WHERE PROCESSNAME IN('DMRI for Equity','DMRI for ETF') " +
				"AND IDTYPE != 2 TSTYPE = " + TsType + " AND TABLENAME = '" + testedTableName + "' LIMIT "+testedIdCounts;
		try {
			list = DBCommons.getDataList(sqlToGetUpdatedPerformanceIdList, Database.Netezza1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis() - startTime;
		System.out.println("[INFO]Got " + list.size() +" performanceids(Individual MODE), TSTYPE = "+ TsType + ",time cost: "+endTime+" ms");
		return list;
	}
	
//	检测Netezza到TsDB更新后的数据量是否保持一致
	public static void compareUpdatedData(TsBlobDataTypeBaseObject tsType, List<String> perfIdList,int bufferSize) throws ParseException{
 		Map<String,Map<String,List<String>>> netezzaValueMap = new HashMap<String,Map<String,List<String>>>();
 		Map<String,Map<String,List<String>>> tsdbValueMap = new HashMap<String,Map<String,List<String>>>();		
		int size = perfIdList.size();
		List<ArrayList<String>> perfIdContainer = new ArrayList<ArrayList<String>>();

//Table DBI_NONTRADEDATES				
		switch(tsType){									
		case tsNonTradingDate:
//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_NonTradingDate(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_NonTradingDate(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of NonTradingDate between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsNonTradingDate =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsNonTradingDate++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsNonTradingDate*bufferSize,size - countTsNonTradingDate*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = NonTradeDatesBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = NonTradeDatesBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_NONTRADEDATES");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListTsdb.isEmpty()){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No byte value found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									if(valueListNetezza.isEmpty()){
										String byteValueStr = valueListTsdb.get(0);
//特殊逻辑，Extra byte默认必须加在TSDB中，且写为3										
										if(!byteValueStr.equals("3")){
											CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Byte value isn't 3 belongs to this date in TSDB!");
											CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
											CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
											CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
											CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Actural ByteValue is: "+byteValueStr);
											CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
										}
									}else{
										System.out.println("[ERROR]Netezza side got unknown value in data list!");
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
		default:
			break;
		}		
	}
	
	@SuppressWarnings("unchecked")
	private static List<ArrayList<String>> getSubList(List<String> perfIdListFull,int bufferSize){
		List<ArrayList<String>> listContainer = new ArrayList<ArrayList<String>>();
		ArrayList<String> list = new ArrayList<String>();
		int flag = 0;
		int buffer = bufferSize;
		int count = 0;
		int size = perfIdListFull.size();
		for(String perfId:perfIdListFull){
			flag++;
			if(flag%buffer !=0){
				list.add(perfId);
			}else{
				list.add(perfId);
				ArrayList<String> sublist = new ArrayList<String>();
				sublist = (ArrayList<String>) list.clone();
				listContainer.add(sublist);
				list.clear();
				flag=0;
				count++;
			}
			if(size-count*buffer>0 && size-count*buffer<buffer){
				for(int j=count*buffer;j<size;j++){
					list.add(perfIdListFull.get(j));
				}
				listContainer.add(list);
				break;
			}
		}
		return listContainer;
	}
	
	private static List<String> getMissingDate(List<String> tsdbDateList,List<String> netezzaDateList) throws ParseException{
		List<String> resultList = new ArrayList<String>();
		List<String> tempList = new ArrayList<String>();
		Date newDate;
		boolean bl = false;
		for(String dateStr:tsdbDateList){
			newDate = df2.parse(dateStr);
			dateStr = df1.format(newDate);
			tempList.add(dateStr);
		}
		bl = netezzaDateList.removeAll(tempList);
		if(bl){
			resultList = netezzaDateList;
		}
		if(!tempList.isEmpty()){
			tempList.clear();
		}
		return resultList;
	}
	
	public static void main(String[] args) throws TSException, Exception{
		List<String> individualPerfIdtsNonTradingDateList = new ArrayList<String>();
//此Table比较特殊，TSDB中会增加一列，名称为ByteValue，此值为日期的类型表示，在此项目中，全部应该都被置成3		
		String tableName = "DBI_NONTRADEDATES";
		String createdOnTime = "2014-06-16";
		int limitCount = 200;
		List<String> performanceIdListEODMode = new ArrayList<String>();

//EOD MODE
		performanceIdListEODMode = getUpdatedPerformanceIdListEODMode(tableName, createdOnTime);
		if(!performanceIdListEODMode.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsNonTradingDate,performanceIdListEODMode,limitCount);
		}else{
			System.out.println("[WARN]PerformanceId(EOD MODE) list is empty,ignore this QA check!");
		}
		
//Individual PerformanceId mode
		individualPerfIdtsNonTradingDateList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsNonTradingDate.TsType,limitCount);
		if(!individualPerfIdtsNonTradingDateList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsNonTradingDate, individualPerfIdtsNonTradingDateList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsMarketPrice list is empty,ignore this QA check!");
		}				
	}
}
