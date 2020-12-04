package org.sqlite.jdbc4;

import org.sqlite.SQLiteConnection;
import org.sqlite.core.CoreStatement;
import org.sqlite.core.DB;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;

public class ResultSetCharacterStreamReader extends Reader
{
    private final CoreStatement stmt;
    private final int col;

    private long pointer = -1;
    private int length = -1;
    private int position = 0;

    public ResultSetCharacterStreamReader(CoreStatement stmt, int col)
    {
        this.stmt = stmt;
        this.col = col;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException
    {
        try {
            if (this.pointer == -1) {
                initializeStream(stmt.getDatbase());
            }
            if (length == position) {
                return -1;
            }

            int read = Math.min(len, length - position);
            byte[] byteBuffer = new byte[cbuf.length];
            stmt.getDatbase().column_text_stream_read(pointer + position, byteBuffer, 0, read);
            new String(byteBuffer, "UTF-8").getChars(0, read, cbuf, off);
            position += read;

            return read;
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
        // I think this can do nothing
    }
}
