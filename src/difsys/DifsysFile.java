/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package difsys;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.fusejna.StructStat;
import net.fusejna.types.TypeMode;

/**
 *
 * @author orachun
 */
public class DifsysFile 
{
	public static final String DB_HOST = Utils.prop("db_host");
	public static final int DB_PORT = Utils.propInt("db_port");
	public static final String DB_NAME = Utils.prop("db_name");
	public static final String COLL_NAME = Utils.prop("collection_name");
	public static final String STORAGE_DIR = Utils.prop("storage_dir");
	public static final int PIECE_SIZE = Utils.propInt("piece_size");
	
	private static DBCollection coll;
	private static final ConcurrentHashMap<String, DifsysFile> files = new ConcurrentHashMap<>();
	
	static
	{
		try
		{
			coll = new Mongo( DB_HOST, DB_PORT ).getDB(DB_NAME).getCollection(COLL_NAME);
		}
		catch (UnknownHostException ex)
		{
			Logger.getLogger(DifsysFile.class.getName()).log(Level.SEVERE, null, ex);
		}
		files.put("/", new DifsysFile("/", true));
	}
	
	public String fullPath;
	public long size;
	public boolean isDir;
	public LinkedList<String> dirContents;
	private File f;
	private ArrayList<byte[]> contents;
	
	
	private DifsysFile(String path, boolean isDir)
	{
		if(files.size() > Utils.propInt("max_cached_files"))
		{
			flushCache();
		}
		this.fullPath = path;
		this.isDir = isDir;
		size = 0;
		f = new File(fullPath);
		if(isDir)
		{
			dirContents = new LinkedList<>();
		}
		else
		{
			contents = new ArrayList<>();
		}
		addDirContent();
	}
	
	public static DifsysFile get(String path)
	{
		return get(path, false, true);
	}
	public static DifsysFile get(String path, boolean isDir, boolean createIfNotExist)
	{
		DifsysFile f = files.get(path);
		if(f == null && createIfNotExist)
		{
			f = readToCache(path);
			if(f == null)
			{
				f = new DifsysFile(path, isDir);
				files.put(path, f);
			}
		}
		return f;
	}
	
	private void addDirContent()
	{
		if(!this.fullPath.equals("/"))
			get(getParent()).dirContents.add(this.fullPath);
	}
	
	public static boolean exists(String path)
	{
		return (files.containsKey(path)) || (coll.findOne(new BasicDBObject("path", path)) != null);
	}
	
	public String getName()
	{
		return f.getName();
	}
	public String getParent()
	{
		return f.getParent();
	}
	protected void getattr(final StructStat.StatWrapper stat)
	{
		stat.setMode(isDir ? TypeMode.NodeType.DIRECTORY : TypeMode.NodeType.FILE);
		stat.size(size);
	}
	
	private byte[] getPieceContent(int pieceNo)
	{
		byte[] content = null;
		try
		{
			content = this.contents.get(pieceNo);
		}
		catch(IndexOutOfBoundsException ex){}
		if(content == null)
		{
			content = new byte[PIECE_SIZE];
			String filename = STORAGE_DIR+fullPath+'.'+(pieceNo);
			FileInputStream fis;
			try
			{
				fis = new FileInputStream(new File(filename));
				fis.read(content);
				fis.close();
			}
			catch (FileNotFoundException ex)
			{}
			catch (IOException ex)
			{
				Logger.getLogger(DifsysFile.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return content;
	}
	
	public long setContent(long offset, long size, ByteBuffer buff)
	{
		Utils.mkdir(STORAGE_DIR+getParent());
		
		//Update size
        if (this.size < offset + size)
		{
            this.size = offset + size;
		}
		
		long total_len = size;
        while (size > 0)
		{
            int content_offset = (int)(offset % PIECE_SIZE);
            long file_offset = offset - content_offset;
            int space_length = PIECE_SIZE - content_offset;
			int pieceNo = (int)(file_offset/PIECE_SIZE);
			byte[] content = getPieceContent(pieceNo);
			int write_length = (int)Math.min(space_length, size);
			buff.get(content, content_offset, write_length);
			offset += write_length;
			size -= write_length;
		}
		return total_len;
	}
	public void getContent(ByteBuffer output,long offset, long size )
	{
        //If the requested size exceed available data size
		size = Math.min(size, this.size - offset);
        while (size > 0)
		{
            int content_offset = (int)(offset % PIECE_SIZE);
            long file_offset = offset - content_offset;
			int pieceNo = (int)(file_offset/PIECE_SIZE);
            byte[] content = getPieceContent(pieceNo);
			int read_size = (int)Math.min(PIECE_SIZE-content_offset, size);
			output.put(content, content_offset, read_size);
            size = size - read_size;
            offset = offset + read_size;
		}
	}
	
	public void delete()
	{
		if(!this.isDir)
		{
			String filenamePrefix = STORAGE_DIR+fullPath;
			try
			{
				Runtime.getRuntime().exec(new String[]{
					"bash", "-c",
					"rm "+filenamePrefix+"*"
				}).waitFor();
			}
			catch (IOException | InterruptedException ex)
			{
				Logger.getLogger(DifsysFile.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		files.remove(fullPath);
		coll.remove(new BasicDBObject("path", fullPath));
	}
	
	private void flushContentCache()
	{
		for(int i=0;i<contents.size();i++)
		{
			String filename = STORAGE_DIR+fullPath+'.'+i;
			FileOutputStream fos;
			try
			{
				fos = new FileOutputStream(filename);
				fos.write(contents.get(i));
				fos.close();
			}
			catch (IOException ex)
			{
				Logger.getLogger(DifsysFile.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		contents.clear();
	}
	
	private static void flushCache()
	{
		for(String path: files.keySet())
		{
			DifsysFile f = files.get(path);
			BasicDBObject obj = new BasicDBObject("path", f.fullPath).
                              append("size", f.size).
                              append("is_dir", f.isDir);
			if(f.isDir)
			{
				BasicDBObject dirContent = new BasicDBObject();
				int i=0;
				for(String cf : f.dirContents)
				{
					dirContent.append(i++ + "", cf);
				}
				obj.append("dir_content", dirContent);
			}
			coll.update(new BasicDBObject("path", f.fullPath), obj, true, true);
			f.flushContentCache();
		}
		files.clear();
	}
	private static DifsysFile readToCache(String path)
	{
		DifsysFile f = null;
		DBObject obj = coll.findOne(new BasicDBObject("path", path));
		if(obj !=null)
		{
			f = new DifsysFile(path, Boolean.parseBoolean(obj.get("is_dir").toString()));
			f.size = Long.parseLong(obj.get("size").toString());
			if(f.isDir)
			{
				DBObject dirContent = (DBObject)obj.get("dir_content");
				f.dirContents.addAll(dirContent.toMap().values());
			}
			files.put(path, f);
		}
		return f;
	}
	
	public static void getContent(String path, long offset, long size, ByteBuffer output)
	{
		DifsysFile.get(path).getContent(output, offset, size);
	}
	public static void setContent(String path, long offset, long size, ByteBuffer input)
	{
		DifsysFile f = DifsysFile.get(path);
		f.setContent(offset, size, input);
	}
}
