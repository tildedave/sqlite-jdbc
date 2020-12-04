package org.sqlite.jdbc4;

import org.sqlite.core.CoreStatement;
import org.sqlite.core.DB;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;

/**
 * {@link Reader} instance for implementation of
 * {@link java.sql.ResultSet#getCharacterStream(int)}
 */
public class ResultSetCharacterStreamReader extends Reader
{
    private final CoreStatement stmt;
    private final int col;

    // Pointer to the start of the text
    private long pointer = -1;
    // Length of the text (in bytes)
    private int length = -1;
    // Current position of the text (in bytes)
    private int position = 0;

    public ResultSetCharacterStreamReader(CoreStatement stmt, int col)
    {
        this.stmt = stmt;
        this.col = col;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException
    {
        DB db = stmt.getDatbase();

        try {
            if (this.pointer == -1) {
                initializeStream(db);
            }

            if (length == position) {
                return -1;
            }

            // length and position are in bytes (8 bit).  Java characters are 16 bytes.
            int readChars = Math.min(len, (length - position) / 2);
            db.column_text_stream_read(pointer + position, cbuf, off, readChars);
            position += (readChars * 2);
            return readChars;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private void initializeStream(DB datbase) throws SQLException
    {
        long[] bytes = datbase.column_text_stream_init(stmt.pointer, this.col);
        this.pointer = bytes[0];
        this.length = (int) bytes[1];
        this.position = 0;
    }

    @Override
    public void close() throws IOException
    {
        this.pointer = -1;
    }
}
