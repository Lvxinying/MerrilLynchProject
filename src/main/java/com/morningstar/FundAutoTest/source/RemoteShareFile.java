package com.morningstar.FundAutoTest.source;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;

import jcifs.smb.SmbFile;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;

import com.morningstar.FundAutoTest.XmlHelper;

public class RemoteShareFile {
	String url = "";
	String user = "";
	String password = "";

	public RemoteShareFile(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
	}

	public RemoteShareFile(String url) {
		this.url = url;
	}

	public static void main(String[] args) {
	}

	public InputStream getRemoteHttpShareFileInputStream() {
		FileSystemOptions opts = new FileSystemOptions();
		FileObject fo = null;
		try {
			fo = VFS.getManager().resolveFile(url, opts);
			InputStream in = fo.getContent().getInputStream();
			XmlHelper.readStream(in);
			return in;
		} catch (FileSystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public String getRemoteHttpShareFileValue(InputStream in, String xPath) {
		String str = XmlHelper.getValueFromInputStream(xPath);
		return str;
	}

	public String getRemoteHttpShareFile(String xPath) {
		FileSystemOptions opts = new FileSystemOptions();
		FileObject fo = null;
		try {
			fo = VFS.getManager().resolveFile(url, opts);
			InputStream in = fo.getContent().getInputStream();
			XmlHelper.readStream(in);
			String str = XmlHelper.getValueFromInputStream(xPath);
			return str;
		} catch (FileSystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public InputStream getRemoteShareFileInputStream() {
		SmbFile smbFile = null;
		try {
			smbFile = new SmbFile("smb://" + user + ":" + password + "@" + url);
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			// save remote share file to input stream
			InputStream in = smbFile.getInputStream();
			XmlHelper.readStream(in);
			return in;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public String getRemoteShareFileValue(InputStream in, String xPath) {
		String str = XmlHelper.getValueFromInputStream(xPath);
		return str;
	}
	
	public int getNodeCount(InputStream in, String xPath) {
		int size = XmlHelper.getNodeCount(xPath);
		return size;
	}

}
