package com.morningstar.FundAutoTest.commons;

import java.sql.Connection;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;


public class DBPool {
	private static Logger logger = LoggerFactory.getLogger(DBPool.class);

	private static BoneCP mssqlConnectionPool = null;
	private static BoneCP mysqlConnectionPool = null;
	private static BoneCP VerticaConnectionPool = null;

	static {
		if (mssqlConnectionPool == null){
			init_MsSQL();
		}
	
		if (VerticaConnectionPool == null){
			init_Vertica();
		}
		
	}

//  初始化数据库连接池
//	修改 Stefan.Hou

	public static void init_MsSQL() {
		BoneCP connectionPool = null;
		try {
			Class.forName(ResourceManager.getMssqlDriverClass());
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(ResourceManager.getMssqlUrl());
			
			config.setUsername("");
			config.setPassword("");
			
			config.setMaxConnectionsPerPartition(ResourceManager.getMaxConnectionsPerPartition());
			config.setMinConnectionsPerPartition(ResourceManager.getMinConnectionsPerPartition());
			config.setPartitionCount(ResourceManager.getPartitionCount());
            
			logger.debug("Start to initialize SQL SERVER database pool...");
			System.out.println("Start to initialize SQL SERVER database pool...");
			long startTime = System.nanoTime();
			connectionPool = new BoneCP(config);
			long endTime = System.nanoTime() - startTime;
			logger.debug("SQL SERVER DataBase initialize finished, total cost [" + endTime / (1000 * 1000) + "]ms");
			System.out.println("SQL SERVER DataBase initialize finished, total cost [" + endTime / (1000 * 1000) + "]ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		mssqlConnectionPool = connectionPool;
	}
	
	public static void init_MySQL() {
		BoneCP connectionPool = null;
		try {
			Class.forName(ResourceManager.getMssqlDriverClass());
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(ResourceManager.getMssqlUrl());
			
			config.setUsername("");
			config.setPassword("");
			
			config.setMaxConnectionsPerPartition(ResourceManager.getMaxConnectionsPerPartition());
			config.setMinConnectionsPerPartition(ResourceManager.getMinConnectionsPerPartition());
			config.setPartitionCount(ResourceManager.getPartitionCount());
            
			logger.debug("Start to initialize MySQL database pool...");
			System.out.println("Start to initialize MySQL database pool...");
			long startTime = System.nanoTime();
			connectionPool = new BoneCP(config);
			long endTime = System.nanoTime() - startTime;
			logger.debug("MySQL DataBase initialize finished, total cost [" + endTime / (1000 * 1000) + "]ms");
			System.out.println("MySQL DataBase initialize finished, total cost [" + endTime / (1000 * 1000) + "]ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		mysqlConnectionPool = connectionPool;
	}

	public static void init_Vertica() {
		BoneCP connectionPool = null;
		try {
			Class.forName(ResourceManager.getVerticaDriverClass());
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(ResourceManager.getVerticaUrl());
			config.setUsername(ResourceManager.getVerticaUsername());
			config.setPassword(ResourceManager.getVerticaPassword());
			config.setMaxConnectionsPerPartition(ResourceManager.getMaxConnectionsPerPartition());
			config.setMinConnectionsPerPartition(ResourceManager.getMinConnectionsPerPartition());
			config.setPartitionCount(ResourceManager.getPartitionCount());

			logger.debug("Start to initialize Vertica database pool...");
			System.out.println("Start to initialize Vertica database pool...");
			long startTime = System.nanoTime();
			connectionPool = new BoneCP(config);
			long endTime = System.nanoTime() - startTime;
			logger.debug("Vertica DataBase initialize finished,total cost [" + endTime / (1000 * 1000) + "]ms");
			System.out.println("Vertica DataBase initialize finished,total cost [" + endTime / (1000 * 1000) + "]ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		VerticaConnectionPool = connectionPool;
	}
	
	public static Connection getConnection_MsSQL() {
		if (mssqlConnectionPool != null)
			try {
				return mssqlConnectionPool.getConnection();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		return null;
	}
	
	public static Connection getConnection_MySQL() {
		if (mysqlConnectionPool != null)
			try {
				return mysqlConnectionPool.getConnection();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		return null;
	}

	public static Connection getConnection_Vertica() {
		if (VerticaConnectionPool != null)
			try {
				return VerticaConnectionPool.getConnection();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		return null;
	}

	public static void shutdownPool(BoneCP connectionPool) {
		connectionPool.shutdown();
	}

}
