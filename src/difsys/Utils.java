/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package difsys;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author orachun
 */
public class Utils 
{
	private static Properties P = new Properties();
	public static void initP(Properties p)
	{
		P.putAll(p);
	}
	public static void initP(String configFileName)
	{
		try
		{
			P.load(new FileInputStream(configFileName));
		}
		catch (IOException ex)
		{
			Logger.getLogger(Difsys.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	public static String prop(String key)
	{
		return P.getProperty(key);
	}
	public static int propInt(String key)
	{
		return Integer.parseInt(P.getProperty(key));
	}
	public static long propLong(String key)
	{
		return Long.parseLong(P.getProperty(key));
	}
	public static double propDouble(String key)
	{
		return Double.parseDouble(P.getProperty(key));
	}
	
	public static String parentDir(String filepath)
	{
		return new File(filepath).getParent();
	}
	public static void mkdir(String dirpath)
	{
		new File(dirpath).mkdirs();
	}
	public static void delete(String filepath)
	{
		new File(filepath).delete();
	}
	public static boolean fileExists(String filepath)
	{
		return new File(filepath).exists();
	}
	public static String fileName(String filepath)
	{
		return new File(filepath).getName();
	}
	public static long time()
	{
		return new Date().getTime()/1000;
	}
	public static boolean isDir(String filepath)
	{
		return new File(filepath).isDirectory();
	}
}
