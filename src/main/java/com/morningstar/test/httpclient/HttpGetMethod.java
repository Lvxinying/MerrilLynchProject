package com.morningstar.test.httpclient;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;



@SuppressWarnings("deprecation")
public class HttpGetMethod {

	/**
	 * @param args
	 */
	private static void show(String message){
		System.out.println(message);
	}
	
	public static void main(String[] args) {
		String url = "http://xoibeta.morningstar.com/XOISuite/IdList.aspx?Package=DataBank&Option=RecvMsgs_Start&ProductGroup=TSProduction";
//创建默认的httpClient实例.
		HttpClient httpclient = new DefaultHttpClient();
//创建HttpGet
		HttpGet httpget = new HttpGet(url);
		show("Executing request URI:" + httpget.getURI());
		try {
//执行HttpGet方法			
			HttpResponse response = httpclient.execute(httpget);
//获取响应
			HttpEntity entity = response.getEntity();
			show("<--------------------------------------------------->");
//如果响应码为200，则打印响应
			if(entity != null){
//打印响应码
				show("Response status code is:" + response.getStatusLine());
//打印响应内容长度				
				show("Response content length is: " + entity.getContentLength());
//打印响应内容
				show("Response content: " + EntityUtils.toString(entity));
				show("<--------------------------------------------------->"); 
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
            //关闭连接,释放资源
            httpclient.getConnectionManager().shutdown();
        }
	}

}
