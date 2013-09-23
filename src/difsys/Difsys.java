/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package difsys;

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
	}
	
	@Override
	protected String[] getOptions() {
		return new String[]{"-o", "max_write="+Utils.prop("max_write"),  "-o", "big_writes"};
	}

	public static void main(final String... args) throws FuseException
	{
		new Difsys().log(false).mount(Utils.prop("mount_dir"));
	}


	@Override
	public int access(final String path, final int access)
	{
		return 0;
	}

	@Override
	public int create(final String path, final TypeMode.ModeWrapper mode, final StructFuseFileInfo.FileInfoWrapper info)
	{
		if (DifsysFile.exists(path)) {
			return -ErrorCodes.EEXIST();
		}
		DifsysFile.get(path);
		return 0;
	}

	@Override
	public int getattr(final String path, final StructStat.StatWrapper stat)
	{
		System.out.println(path);
		DifsysFile f = DifsysFile.get(path, true, false);
		if(f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		f.getattr(stat);
		return 0;
	}

	@Override
	public int mkdir(final String path, final TypeMode.ModeWrapper mode)
	{
		if (DifsysFile.exists(path)) {
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
		if(f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		if (f.isDir) {
			return -ErrorCodes.EISDIR();
		}
		f.getContent(buffer, offset, size);
		return 0;
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler)
	{
		DifsysFile f = DifsysFile.get(path, true, false);
		if(f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		if (!f.isDir) {
			return -ErrorCodes.ENOTDIR();
		}
		filler.add(f.dirContents);
		return 0;
	}


	@Override
	public int rmdir(final String path)
	{
		DifsysFile f = DifsysFile.get(path, true, false);
		if(f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		if (!f.isDir) {
			return -ErrorCodes.ENOTDIR();
		}
		if(f.dirContents.size()>0)
		{
			return -ErrorCodes.ENOTEMPTY();
		}
		f.delete();
		return 0;
	}

	@Override
	public int unlink(final String path)
	{
		DifsysFile f = DifsysFile.get(path, true, false);
		if(f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		if (f.isDir) {
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
		if(f == null)
		{
			return -ErrorCodes.ENOENT();
		}
		if (f.isDir) {
			return -ErrorCodes.EISDIR();
		}
		f.setContent(writeOffset, bufSize, buf);
		return 0;
	}
}