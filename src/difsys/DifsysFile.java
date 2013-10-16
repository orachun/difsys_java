/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package difsys;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.TreeSet;
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
	private static int count = 0;
	private int id = count++;
	public static final String DB_HOST = Utils.prop("fs_db_host");
	public static final int DB_PORT = Utils.propInt("fs_db_port");
	public static final String DB_NAME = Utils.prop("fs_db_name");
	public static final String COLL_NAME = Utils.prop("fs_collection_name");
	public static final String STORAGE_DIR = Utils.prop("fs_storage_dir");
	public static final int PIECE_SIZE = Utils.propInt("fs_piece_size");
	
	private static int cachedPieces = 0;
	private static DBCollection coll;
	private static final ConcurrentHashMap<String, DifsysFile> files = new ConcurrentHashMap<>();
//	private static final ConcurrentLinkedQueue<String> deletedFiles = new ConcurrentLinkedQueue<>();
	private static DifsysFile root;
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
	}
	public static void init()
	{
		root = DifsysFile.get("/", true, true);
	}
	
	
	private String fullPath;
	public long size;
	public boolean isDir;
	public HashSet<String> dirContents;
	private File f;
	private ConcurrentHashMap<Integer, PieceContent> contents;
	private boolean noFlush = false;
	public final Object FILEPIECE_LOCK = new Object();
	private boolean isDeleted = false;
	
	private long ctime = Utils.time();
	private long atime = Utils.time();
	private long mtime = Utils.time();
//	public long mode;
//	public int uid;
//	public int gid;
	
	
	private DifsysFile(String path, boolean isDir)
	{
		if(files.size() > Utils.propInt("fs_max_cached_files") || 
				cachedPieces*(long)PIECE_SIZE > Utils.propLong("fs_max_cached_content"))
		{
			flushCache(false);
		}
		this.fullPath = path;
		this.isDir = isDir;
		size = 0;
		f = new File(fullPath);
		if(isDir)
		{
			dirContents = new HashSet<>();
		}
		else
		{
			contents = new ConcurrentHashMap<>();
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
		if(f == null)
		{
//			if(!deletedFiles.contains(path))
//			{
//				f = readToCache(path);
//			}
			f = readToCache(path);
			if(f == null && createIfNotExist)
			{
				f = new DifsysFile(path, isDir);
			}
			if(f != null)
			{
//				deletedFiles.remove(path);
				files.put(path, f);
			}
		}
		return f;
	}
	public void setPath(String fullPath)
	{
		this.fullPath = fullPath;
		this.f = new File(fullPath);
	}
	private void addDirContent()
	{
		if(!this.fullPath.equals("/"))
		{
			this.noFlush = true;
			DifsysFile parent = DifsysFile.get(getParent(), true, false);
			parent.dirContents.add(this.getName());
			this.noFlush = false;
		}
	}
	
	public void move(String newName)
	{
		final String oldPath = this.fullPath;
		files.remove(fullPath);
		DifsysFile oldParent = DifsysFile.get(f.getParent(), true, false);
		oldParent.dirContents.remove(f.getName());

		FileFilter ff = new FileFilter() {

			@Override
			public boolean accept(File pathname)
			{
				return pathname.getAbsolutePath().replace(STORAGE_DIR, "").startsWith(oldPath);
			}
		};
		for(File pf : new File(STORAGE_DIR).listFiles(ff))
		{
			String newFilePath = pf.getAbsolutePath().replace(oldPath, newName);
			pf.renameTo(new File(newFilePath));
		}
		
		
		this.setPath(newName);
		this.addDirContent();
		files.put(newName, this);
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
		stat.atime(atime);
		stat.ctime(ctime);
		stat.mtime(mtime);
	}
	
	public PieceContent getPieceContent(int pieceNo)
	{
		if(cachedPieces*(long)PIECE_SIZE > Utils.propLong("fs_max_cached_content"))
		{
			this.flushContentCache(pieceNo);
		}
		PieceContent content = this.contents.get(pieceNo);
		if(content == null)
		{
			content = new PieceContent();
			
			//If this piece has content
			if(pieceNo < Math.ceil(this.size / (double)PIECE_SIZE))
			{
				String filename = STORAGE_DIR+fullPath+'.'+(pieceNo);
				FileInputStream fis;
//				synchronized(FILEPIECE_LOCK)
//				{
//					noFlush = true;
//					while(!Utils.fileExists(filename))
//					{
//						try
//						{
//							FILEPIECE_LOCK.wait(Utils.propInt("fs_piece_wait_timeout"));
//						}
//						catch (InterruptedException ex){}
//					}
//					noFlush = false;
//				}
				if(Utils.fileExists(filename))
				{
					try
					{
						fis = new FileInputStream(new File(filename));
						fis.read(content.content);
						fis.close();
					}
					catch (IOException ex)
					{
						Logger.getLogger(DifsysFile.class.getName()).log(Level.SEVERE, null, ex);
					}
				}
			}
			
			//If this is a new piece for writing content into
			else
			{
				cachedPieces++;
			}
			this.contents.put(pieceNo, content);
		}
		return content;
	}
	
	public long setContent(final long offset, final long size, ByteBuffer buff)
	{
		Utils.mkdir(STORAGE_DIR+getParent());
		
		long total_len = size;
		long write_offset = offset;
		long write_size = size;
        while (write_size > 0)
		{
            int content_offset = (int)(write_offset % PIECE_SIZE);
            long file_offset = write_offset - content_offset;
            int space_length = PIECE_SIZE - content_offset;
			int pieceNo = (int)(file_offset/PIECE_SIZE);
			int write_length = (int)Math.min(space_length, write_size);
			PieceContent content = getPieceContent(pieceNo);
			buff.get(content.content, content_offset, write_length);
			content.edited = true;
			write_size -= write_length;
			write_offset += write_length;
		}
		
		//Update size
        if (this.size < offset + size)
		{
            this.size = offset + size;
		}
		
		mtime = Utils.time();
		atime = mtime;
		return total_len;
	}
	public int getContent(ByteBuffer output,long offset, long size )
	{
        //If the requested size exceed available data size
		size = Math.min(size, this.size - offset);
		int read = 0;
        while (size > 0)
		{
            int content_offset = (int)(offset % PIECE_SIZE);
            long file_offset = offset - content_offset;
			int pieceNo = (int)(file_offset/PIECE_SIZE);
            PieceContent content = getPieceContent(pieceNo);
			int read_size = (int)Math.min(PIECE_SIZE-content_offset, size);
			output.put(content.content, content_offset, read_size);
            size = size - read_size;
            offset = offset + read_size;
			read += read_size;
		}
		atime = Utils.time();
		return read;
	}
	
	public void truncate(long offset)
	{
//		int content_offset = (int)(offset % PIECE_SIZE);
//		long file_offset = offset - content_offset;
//		int pieceNo = (int)(file_offset/PIECE_SIZE);
//		if(offset < size)
//		{
//			for(int i=pieceNo;i<contents.size();i++)
//			{
//				contents.remove(i);
//				Utils.delete(STORAGE_DIR+fullPath+'.'+(pieceNo));
//			}
//		}
//		else
//		{
//			for(int i=contents.size();i<pieceNo+1;i++)
//			{
//				getPieceContent(i).edited = true;
//			}
//		}
		size = offset;
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
		else
		{
			Utils.delete(STORAGE_DIR+fullPath);
		}
		DifsysFile parent = DifsysFile.get(this.getParent(), true, false);
		//System.out.println("Remove "+this.getName()+" from "+parent.fullPath);
		parent.dirContents.remove(this.getName());
		//parent.updateToDB();
		files.remove(fullPath);
//		deletedFiles.add(fullPath);
		if(this.isDir)
		{
			this.dirContents.clear();
		}
		else
		{
			this.contents.clear();
		}
		System.gc();
		//System.out.println("Remove "+fullPath+" from DB.");
		coll.remove(new BasicDBObject("path", fullPath));
	}
	
	private void flushContentCache(int except)
	{
		for(int i : contents.keySet())
		{
			String filename = STORAGE_DIR+fullPath+'.'+i;
			FileOutputStream fos;
			try
			{
				PieceContent content = contents.get(i);
				if(content != null && content.edited)
				{
					fos = new FileOutputStream(filename);
					int writeLength = (int)(Math.min((i+1)*PIECE_SIZE, size)-i*PIECE_SIZE);
					fos.write(content.content, 0, writeLength);
					fos.close();
				}
			}
			catch (IOException ex)
			{
				Logger.getLogger(DifsysFile.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		cachedPieces-=contents.size();
		PieceContent exceptPiece = contents.get(except);
		contents.clear();
		if(exceptPiece!=null)
		{
			contents.put(except, exceptPiece);
			cachedPieces++;
		}
		System.gc();
	}
	
	private BasicDBObject toBson()
	{
		BasicDBObject obj = new BasicDBObject("path", this.fullPath)
				.append("size", this.size)
				.append("is_dir", this.isDir)
				.append("ctime", ctime)
				.append("atime", atime)
				.append("mtime", mtime);
		if (this.isDir)
		{
			BasicDBList dirContent = new BasicDBList();
			for (String cf : this.dirContents)
			{
				dirContent.add(cf);
			}
			obj.append("dir_content", dirContent);
		}
		return obj;
	}
	private void updateToDB()
	{
		//System.out.println("Save "+fullPath+" to DB.");
		coll.update(new BasicDBObject("path", this.fullPath), this.toBson(), true, false);
	}
	
	public synchronized static void flushCache(boolean all)
	{
		long time = Utils.time();
		long last_access_to_flush = Utils.propLong("fs_last_access_to_flush");
		long duration = Utils.propLong("fs_flush_time_limit");
		LinkedList<String> remove = new LinkedList<>();
		for(String path: files.keySet())
		{
			DifsysFile f = files.get(path);
			if(!all && !f.noFlush && time - f.atime < last_access_to_flush)
			{
				continue;
			}
			//System.out.println("Flushing "+f.fullPath);
			f.updateToDB();
			if(!f.isDir)
			{
				f.flushContentCache(-1);
			}
			remove.add(f.fullPath);
			if(!all && Utils.time() - time > duration)
			{
				break;
			}
		}
		for(String path: remove)
		{
			files.remove(path);
		}
		files.put("/", root);
		System.gc();
		
		
//		if(deletedFiles.size() > Utils.propInt("fs_max_cached_files"))
//		{
//			BasicDBList in = new BasicDBList();
//			for(String deletedPath : deletedFiles)
//			{
//				in.add(deletedPath);
//			}
//			coll.remove(new BasicDBObject("path", new BasicDBObject("$in", in)));
//		}
	}
	private static DifsysFile readToCache(String path)
	{
		DifsysFile f = null;
		DBObject obj = coll.findOne(new BasicDBObject("path", path));
		if(obj !=null)
		{
			f = new DifsysFile(path, Boolean.parseBoolean(obj.get("is_dir").toString()));
			f.size = Long.parseLong(obj.get("size").toString());
			f.ctime = Long.parseLong(obj.get("ctime").toString());
			f.mtime = Long.parseLong(obj.get("mtime").toString());
			f.atime = Long.parseLong(obj.get("atime").toString());
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
	public static void printStatus()
	{
		System.out.println("Cached Files: "+files.size());
		System.out.printf("Cached Content: %.2f MB\n", cachedPieces*PIECE_SIZE/1024D/1024D);
//		System.out.println("Files to delete: "+deletedFiles.size());
	}
	
	public static void notifyPieceCreated(String path)
	{
		DifsysFile f = DifsysFile.get(path);
		synchronized(f.FILEPIECE_LOCK)
		{
			f.FILEPIECE_LOCK.notifyAll();
		}
	}
	
	public static void addFile(String path, long size)
	{
		DifsysFile f = DifsysFile.get(path, false, true);
		f.size = size;
		f.addDirContent();
	}
}
