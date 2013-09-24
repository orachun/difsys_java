/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package difsys;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.StructFuseFileInfo;
import net.fusejna.StructStat;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode;
import net.fusejna.util.FuseFilesystemAdapterAssumeImplemented;

/**
 *
 * @author orachun
 */
public class Difsys extends FuseFilesystemAdapterAssumeImplemented
{

	ConcurrentHashMap<String, StatWrapper> attrs = new ConcurrentHashMap<>();

	public Difsys()
	{
		Utils.initP();
		Utils.mkdir(Utils.prop("storage_dir"));
		Utils.mkdir(Utils.prop("mount_dir"));
		DifsysFile.init();
	}

	@Override
	protected String[] getOptions()
	{
		return new String[]
		{
			"-o", "max_write=" + Utils.prop("max_write"), "-o", "big_writes"
		};
	}

	public static void main(final String... args) throws FuseException
	{
		new Difsys().log(false).mount(Utils.prop("mount_dir"));
	}
	
	public void cmd(String path)
	{
		String filename = Utils.fileName(path).replace(Utils.prop("cmd_prefix"), "");
		if(filename.startsWith("status"))
		{
			DifsysFile.printStatus();
		}
		else if(filename.startsWith("flush"))
		{
			DifsysFile.flushCache();
		}
		else if(filename.startsWith("gc"))
		{
			System.gc();
		}
		else if(filename.startsWith("piece_created"))
		{
			filename = path.replace(Utils.prop("cmd_prefix"), "").replace("piece_created.", "");
			DifsysFile f = DifsysFile.get(filename);
			synchronized(f.FILEPIECE_LOCK)
			{
				f.FILEPIECE_LOCK.notifyAll();
			}
		}
	}

	
	
	@Override
	public void afterUnmount(final File mountPoint)
	{
		DifsysFile.flushCache();
	}
	
	@Override
	public int access(final String path, final int access)
	{
		return 0;
	}

	@Override
	public int create(final String path, final TypeMode.ModeWrapper mode, final StructFuseFileInfo.FileInfoWrapper info)
	{
		if(Utils.fileName(path).startsWith(Utils.prop("cmd_prefix")))
		{
			cmd(path);
			return 0;
		}
		if (DifsysFile.exists(path))
		{
			return -ErrorCodes.EEXIST();
		}
		DifsysFile.get(path);
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

		System.out.println("Rename is not implemented.");
		return 0;
	}

	@Override
	public int rmdir(final String path)
	{

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
