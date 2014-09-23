package com.morningstar.FundAutoTest.commons;

import java.io.*;
import java.util.Date;
import com.ibm.icu.text.SimpleDateFormat;

public class CustomizedLog {

	/**
	 * @param args
	 * @author stefan.hou
	 * @throws IOException 
	 * @date:2013-10-24
	 */
	public static void creatCusomizedLogFile(String filePath,String fileName,String topic) throws IOException{
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currenTime = df.format(new Date());
		File file = new File(filePath);
		if(file.exists() == false){
			file.mkdirs();
		}
		else{
			FileWriter writer = new FileWriter(filePath + fileName);
			writer.write("Log Topic:" + topic + "\n");
			writer.append("Testing Time:" + currenTime + "\n");
			writer.append("===================================================================" + "\n");
			writer.close();
		}
	}
/*	public static void writeCustomizedLogFile(String filePath,String content) throws IOException{
		RandomAccessFile randomWriter = new  RandomAccessFile (filePath,"rw");
		long fileLength = randomWriter.length();
//定位到文件最后一行
		randomWriter.seek(fileLength);
		randomWriter.writeUTF(content + "\n");
		randomWriter.close();
	}
*/	
	public static void writeCustomizedLogFile(String filePath, String content) {  
        try {  
            // 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件   
            FileWriter writer = new FileWriter(filePath, true);  
            writer.append(content+"\n");  
            writer.close();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }
}
