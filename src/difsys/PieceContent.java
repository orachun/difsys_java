/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package difsys;

import static difsys.DifsysFile.PIECE_SIZE;

/**
 *
 * @author orachun
 */
public class PieceContent
{
	byte[] content = new byte[PIECE_SIZE];
	boolean edited = false;
	
	public byte[] getContent()
	{
		return content;
	}
}
