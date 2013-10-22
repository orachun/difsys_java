/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package difsys;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;
import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.StructFuseFileInfo;
import net.fusejna.StructStat;
import net.fusejna.types.TypeMode;
import net.fusejna.util.FuseFilesystemAdapterAssumeImplemented;

/**
 *
 * @author orachun
 */
public class Difsys extends FuseFilesystemAdapterAssumeImplemented
{
	public final String CMD_PREFIX;

	public Difsys(String rootDir, String configFileName) throws FuseException
	{
		System.out.println("Initializing file system...");
		Utils.initP(configFileName);
		Utils.mkdir(Utils.prop("fs_storage_dir"));
		Utils.mkdir(rootDir);
		CMD_PREFIX = Utils.prop("fs_cmd_prefix");
		DifsysFile.init();
		DifsysFile.get("/");
		this.log(false).mount(new File(rootDir), false);
		System.out.println("Ready.");
	}
	
	public Difsys(String rootDir, Properties p) throws FuseException
	{
		System.out.println("Initializing file system...");
		Utils.initP(p);
		Utils.mkdir(Utils.prop("fs_storage_dir"));
		Utils.mkdir(rootDir);
		CMD_PREFIX = Utils.prop("fs_cmd_prefix");
		DifsysFile.init();
		DifsysFile.get("/");
		this.log(false).mount(new File(rootDir), false);
		System.out.println("Ready.");
	}

	@Override
	protected String[] getOptions()
	{
		return new String[]
		{
			"-o", "max_write=" + Utils.prop("fs_max_write"), "-o", "big_writes"
		};
	}
//
//	public static void main(final String... args) throws FuseException
//	{
//		new Difsys();
//	}
	
	public void cmd(String path)
	{
		String filename = Utils.fileName(path).replace(CMD_PREFIX, "");
		if(filename.startsWith("status"))
		{
			DifsysFile.printStatus();
		}
		else if(filename.startsWith("flush"))
		{
			DifsysFile.flushCache(true);
		}
		else if(filename.startsWith("gc"))
		{
			System.gc();
		}
		else if(filename.startsWith("piece_created"))
		{
			filename = path.replace(CMD_PREFIX, "").replace("piece_created.", "");
			DifsysFile.notifyPieceCreated(filename);
		}
	}

	
	
	@Override
	public void afterUnmount(final File mountPoint)
	{
		DifsysFile.flushCache(true);
	}
	
	@Override
	public int access(final String path, final int access)
	{
		return 0;
	}

	@Override
	public int create(final String path, final TypeMode.ModeWrapper mode, final StructFuseFileInfo.FileInfoWrapper info)
	{
		if(Utils.fileName(path).startsWith(CMD_PREFIX))
		{
			cmd(path);
			return 0;
		}
		if (DifsysFile.exists(path))
		{
			return -ErrorCodes.EEXIST();
		}
		DifsysFile.get(path, false, true);
		//System.out.println("Create "+path);
		return 0;
	}

	@Override
	public int getattr(final String path, final StructStat.StatWrapper stat)
	{
		DifsysFile f = DifsysFile.get(path, true, false);
		if (f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		f.getattr(stat);
		return 0;
	}

	@Override
	public int mkdir(final String path, final TypeMode.ModeWrapper mode)
	{
		//System.out.println("$:mkdir "+path);
		if (DifsysFile.exists(path))
		{
			return -ErrorCodes.EEXIST();
		}
		DifsysFile.get(path, true, true);
		return 0;
	}

	@Override
	public int open(final String path, final StructFuseFileInfo.FileInfoWrapper info)
	{
		return 0;
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final StructFuseFileInfo.FileInfoWrapper info)
	{
		DifsysFile f = DifsysFile.get(path, false, false);
		if (f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		if (f.isDir)
		{
			return -ErrorCodes.EISDIR();
		}
		return f.getContent(buffer, offset, size);
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler)
	{
		//System.out.println("$:ls "+path);
		DifsysFile f = DifsysFile.get(path, true, false);
		if (f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		if (!f.isDir)
		{
			return -ErrorCodes.ENOTDIR();
		}
		
		filler.add(f.dirContents);
		return 0;
	}

	@Override
	public int rename(final String path, final String newName)
	{
		//System.out.println("$:mv "+path+" "+newName);
		DifsysFile f = DifsysFile.get(path, true, false);
		if (f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		DifsysFile newParent = DifsysFile.get(Utils.parentDir(newName), true, false);
		if (newParent == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!newParent.isDir) {
			return -ErrorCodes.ENOTDIR();
		}
		
		DifsysFile oldParent = DifsysFile.get(f.getParent(), true, false);
		oldParent.dirContents.remove(f.getName());
		f.move(newName);
		return 0;
	}

	@Override
	public int rmdir(final String path)
	{
		//System.out.println("$:rmdir "+path);
		DifsysFile f = DifsysFile.get(path, true, false);
		if (f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		if (!f.isDir)
		{
			return -ErrorCodes.ENOTDIR();
		}
		if (f.dirContents.size() > 0)
		{
			return -ErrorCodes.ENOTEMPTY();
		}
		f.delete();
		return 0;
	}

	@Override
	public int truncate(final String path, final long offset)
	{

		DifsysFile f = DifsysFile.get(path, true, false);
		if (f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		if (f.isDir)
		{
			return -ErrorCodes.EISDIR();
		}
		f.truncate(offset);
		return 0;
	}

	@Override
	public int unlink(final String path)
	{
		//System.out.println("$:unlink "+path);
		DifsysFile f = DifsysFile.get(path, true, false);
		if (f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		if (f.isDir)
		{
			return -ErrorCodes.EISDIR();
		}
		f.delete();
		//System.out.println("Del "+path);
		return 0;
	}

	@Override
	public int write(final String path, final ByteBuffer buf, final long bufSize, final long writeOffset,
			final StructFuseFileInfo.FileInfoWrapper wrapper)
	{
		DifsysFile f = DifsysFile.get(path, true, false);
		if (f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		if (f.isDir)
		{
			return -ErrorCodes.EISDIR();
		}
		f.setContent(writeOffset, bufSize, buf);
		return (int) bufSize;
	}
}
