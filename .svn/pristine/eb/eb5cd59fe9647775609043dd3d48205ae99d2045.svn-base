package com.morningstar.FundAutoTest.commons;

//数据库相关，通过循环建立连接，为Database的枚举做准备
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class DBCommons {

//	private Connection con = null;


	public static String getData(String sql, Database database) throws SQLException {
		List<String> list = getDataList(sql, database);
		if (list.size() > 0)
			return list.get(0);
		return null;
	}

	public static List<String> getDataList(String sql, Database database) throws SQLException {
		return getDataList(database, sql, null);
	}
	
	public static HashMap<String,String> getDataHashMap(String sql, Database database) throws Exception{
		return getDataHashMap(database, sql, null);
	}
	
	public static List<Map<Integer , String>> getDataListMapIntStr(Database database,String sql) throws Exception{
		return getDataListMapIntStr(database,sql,null);
	}
	
//	public static List<Map<String, String>> getDataListMapStrStr(Database database,String sql) throws Exception{
//		return getDataListMapStrStr(database,sql,null);
//	}

//注意：RepeatKeyContainer 为一个辅助加载重复Key值的类	
	public static IdentityHashMap<Object, String> getDataIdentityHashMap(String sql, Database database) throws Exception{
		return getDataIdentityHashMap(database, sql, null);
	}

//	获取数据库信息并存在List中
	public static List<String> getDataList(Database database, String sql, List<String> input) throws SQLException {
		Connection con = null;
		con = DBFreshpool.getConnection(database);
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		List<String> list = new ArrayList<String>();
		if (con != null) {		
			try {
				pstmt = con.prepareStatement(sql);
				if (input != null && input.size() > 1) {
					for (int i = 1; i <= input.size(); i++) {
						pstmt.setString(i, input.get(i-1));
					}
				}
				rs = pstmt.executeQuery();
				while (rs.next()) {
					int size = rs.getMetaData().getColumnCount();
					for (int i = 1; i <= size; i++) {
						list.add("".equals(rs.getString(i)) ? null : rs.getString(i));
					}					
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}finally {
				if(rs !=null){
					rs.close();
				}
				if(pstmt != null ){
					pstmt.close();
				}			
				if (con != null) {
					try {				
						con.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} 
		}
		return list;
	}
	
//	获取数据库信息并存在HashMap中(仅针对于有两列的情况)
	public static HashMap<String,String> getDataHashMap(Database database, String sql, List<String> input) throws SQLException {
		Connection con = null;
		con = DBFreshpool.getConnection(database);
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		HashMap<String, String> map = new HashMap<String, String>();
		if (con != null) {
			try {
				pstmt = con.prepareStatement(sql);
				if (input != null && input.size() > 1) {
					for (int i = 1; i <= input.size(); i++) {
						pstmt.setString(i, input.get(i-1));
					}
				}
				rs = pstmt.executeQuery();
				while (rs.next()) {
					map.put(rs.getString(1), rs.getString(2));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if(rs != null){
					rs.close();
				}
				if(pstmt != null){
					pstmt.close();
				}								
				if (con != null) {
					try {				
						con.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} 	
		}
		return map;
	}
	
//	获取数据库信息并存在HashMap中(仅针对于有两列的情况)
	public static IdentityHashMap<Object,String> getDataIdentityHashMap(Database database, String sql, List<String> input) throws SQLException {
		Connection con = null;
		con = DBFreshpool.getConnection(database);
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		IdentityHashMap<Object, String> map = new IdentityHashMap<Object, String>();
		if (con != null) {
			try {
				pstmt = con.prepareStatement(sql);
				if (input != null && input.size() > 1) {
					for (int i = 1; i <= input.size(); i++) {
						pstmt.setString(i, input.get(i-1));
					}
				}
				rs = pstmt.executeQuery();
				while (rs.next()) {
					map.put(new RepeatKeyContainer(rs.getString(1)), rs.getString(2));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if(rs != null){
					rs.close();
				}
				if(pstmt != null){
					pstmt.close();
				}								
				if (con != null) {
					try {				
						con.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} 	
		}
		return map;
	}

//	获取数据库表中的数据并存在ListMap中，参数：database，sql，input
//	说明：input为SQL条件语句的参数值，以STRING数组形式保存，比如SELECT * FROM DB TABLE WHERE A=? AND B=? AND C=?，参数值替换?部分
	public static List<Map<Integer, String>> getDataListMapIntStr(Database database, String sql, String[] input) throws SQLException {
			Connection con = null;
			ResultSet rs = null;
			PreparedStatement pstmt =null;
			List<Map<Integer, String>> list = new ArrayList<Map<Integer, String>>();
			con = DBFreshpool.getConnection(database);
		try {
			pstmt = con.prepareStatement(sql);
			for (int i = 1; i <= input.length; i++) {
				pstmt.setString(i, input[i - 1]);
			}
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Map<Integer, String> map = new HashMap<Integer, String>();
				int size = rs.getMetaData().getColumnCount();
				for (int i = 1; i <= size; i++)
					map.put(i, rs.getString(i));
				list.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(rs != null){
				rs.close();
			}
			if(pstmt != null){
				pstmt.close();
			}	
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return list;
	}

//	public static Map<String,List<String>> getDataMapList(Database database, String sql, String[] input) throws SQLException {
//		Connection con = null;
//		ResultSet rs = null;
//		PreparedStatement pstmt =null;
//		Map<String,List<String>> mapInnerList = new HashMap<String,List<String>>();
//		con = DBFreshpool.getConnection(database);
//	try {
//		pstmt = con.prepareStatement(sql);
//		for (int i = 1; i <= input.length; i++) {
//			pstmt.setString(i, input[i - 1]);
//		}
//		rs = pstmt.executeQuery();
//		while (rs.next()) {
//			mapInnerList.put(rs.getString(1),);
//			Map<Integer, String> map = new HashMap<Integer, String>();
//			int size = rs.getMetaData().getColumnCount();
//			for (int i = 1; i <= size; i++)
//				map.put(i, rs.getString(i));
//			list.add(map);
//		}
//	} catch (Exception e) {
//		e.printStackTrace();
//	}finally {
//		if(rs != null){
//			rs.close();
//		}
//		if(pstmt != null){
//			pstmt.close();
//		}	
//		if (con != null) {
//			try {
//				con.close();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//	}
//	return list;
//}
	
//Stefan.Hou更新，2013-09-27	
//	获取数据库的列数
	public static int getColumnCount(Database database,String sql) throws SQLException{
		    Connection con = null;
		    PreparedStatement pstmt = null;
		    ResultSet rs = null;
			int columnCount = 0;
			con = DBFreshpool.getConnection(database);		
		try {
			pstmt = con.prepareStatement(sql);
			rs = pstmt.executeQuery();
			columnCount = rs.getMetaData().getColumnCount();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(rs != null){
				rs.close();
			}
			if(pstmt != null){
				pstmt.close();
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} 
		return columnCount;
	}
	
//	返回指定列号的列名
	public static String getColumnName(Database database , String sql , int columnNum) throws SQLException{
		Connection con = null;
		String columnName = null;
		con = DBFreshpool.getConnection(database);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = con.prepareStatement(sql);
			rs = pstmt.executeQuery();
			columnName = rs.getMetaData().getColumnName(columnNum);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
				try {
					if(rs != null){
						rs.close();
					}
					if(pstmt != null){
						pstmt.close();
					}
					if(con != null){
						con.close();
					}					
				} catch (Exception e) {
					e.printStackTrace();
					}
				} 
		return columnName;
	}

//	返回指定列号的列数据类型
	public static String getColumnDataTypeName(Database database , String sql , int columnNum) throws SQLException{
		Connection con = null;
		String columnDataType = null;
		con = DBFreshpool.getConnection(database);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = con.prepareStatement(sql);
			rs = pstmt.executeQuery();
			columnDataType = rs.getMetaData().getColumnTypeName(columnNum);
            rs.close();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
				try {
					if(rs != null){
						rs.close();
					}
					if(pstmt != null){
						pstmt.close();
					}
					if(con != null){
						con.close();
					}	
				} catch (Exception e) {
					e.printStackTrace();
				}
		} 
		return columnDataType;
	}	
	
//获取数据库表中的某个单元格位置的数据格式，返回值为一个List
	public static List<Map<String, String>> getDataTypeListMap(Database database, String sql , int columnNum ) throws SQLException {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs =  null;
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		con = DBFreshpool.getConnection(database);			
		try {
			pstmt = con.prepareStatement(sql);
			rs = pstmt.executeQuery();
			String columnName = rs.getMetaData().getColumnName(columnNum);
			String dbDataType = rs.getMetaData().getColumnTypeName(columnNum);	
			Map<String, String> map = new HashMap<String, String>();
			map.put(columnName, dbDataType);
			list.add(map);
			rs.close();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace(); 
		}finally {
				try {
					if(rs != null){
						rs.close();
					}
					if(pstmt != null){
						pstmt.close();
					}
					if(con != null){
						con.close();
					}	
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
		return list;
		}
//返回指定列号的列数据大小
		public static int getColumnDataSize(Database database , String sql , int columnNum) throws SQLException{
			Connection con = null;
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			int columnDataSize = 0;
			con = DBFreshpool.getConnection(database);				
			try {
				pstmt = con.prepareStatement(sql);
				rs = pstmt.executeQuery();
			    columnDataSize = rs.getMetaData().getColumnDisplaySize(columnNum);
			} catch (Exception e) {
				e.printStackTrace();
			}finally {
					try {
						if(rs != null){
							rs.close();
						}
						if(pstmt != null){
							pstmt.close();
						}
						if(con != null){
							con.close();
						}	
					} catch (Exception e) {
						e.printStackTrace();
					}
			} 
			return columnDataSize;	
		}
		
//Stefan.Hou更新,2013-11-29
//List嵌套Map，Map中key value都是String类型的，仅针对数据库返回两列的情况		
		public static List<Map<String, String>> getDataListMapStrStr(Database database, String sql) throws SQLException {
			Connection con = null;
			ResultSet rs = null;
			PreparedStatement pstmt =null;
			List<Map<String, String>> list = new ArrayList<Map<String, String>>();
			con = DBFreshpool.getConnection(database);
		try {
			pstmt = con.prepareStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Map<String, String> map = new HashMap<String, String>();
				map.put(rs.getString(1), rs.getString(2));
				list.add(map);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
	    if(rs != null){
	    	rs.close();
    	}
		if(pstmt != null){
			pstmt.close();
		}	
		if (con != null) {
			try {
				con.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		}
		return list;
	}		
		
//Stefan.Hou更新，2013-10-15
//执行SQL语句，注意：应用Statement而不是PrepareStatement		
		public static void executeSQL(Database database , String sql) throws SQLException{
				Connection con = null;
				Statement stmt = null;
				con = DBFreshpool.getConnection(database);			
			try {
				stmt = con.createStatement();
				stmt.close();
			} catch (Exception e) {
				e.printStackTrace();
			}finally {
					try {
						if(stmt != null){
							stmt.close();
						}
						if(con != null){
							con.close();
						}	
					} catch (Exception e) {
						e.printStackTrace();
					}
			} 
		}
	}

class RepeatKeyContainer{ 
    private String keyValue ; 
    public RepeatKeyContainer(String keyValue){ 
        this.keyValue = keyValue ; 
    } 
    public boolean equals(Object obj){ 
        if(this==obj){ 
            return true ; 
        } 
        if(!(obj instanceof RepeatKeyContainer)){ 
            return false ; 
        } 
        RepeatKeyContainer rkc = (RepeatKeyContainer)obj ; 
        if(this.keyValue.equals(rkc.keyValue)){ 
            return true ; 
        }else{ 
            return false ; 
        } 
    } 
    public int hashCode(){ 
        return this.keyValue.hashCode(); 
    } 
    public String toString(){ 
        return this.keyValue; 
    } 
};
