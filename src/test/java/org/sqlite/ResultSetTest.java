package org.sqlite;

import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ResultSetTest {

    private Connection conn;
    private Statement stat;

    @Before
    public void connect() throws Exception {
        conn = DriverManager.getConnection("jdbc:sqlite:");
        stat = conn.createStatement();
        stat.executeUpdate("create table test (id int primary key, DESCRIPTION varchar(40), fOo varchar(3));");
        stat.executeUpdate("insert into test values (1, 'description', 'bar')");
    }

    @After
    public void close() throws SQLException {
        stat.close();
        conn.close();
    }

    @Test
    public void testTableColumnLowerNowFindLowerCaseColumn()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select * from test");
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.findColumn("id"));
    }

    @Test
    public void testTableColumnLowerNowFindUpperCaseColumn()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select * from test");
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.findColumn("ID"));
    }

    @Test
    public void testTableColumnLowerNowFindMixedCaseColumn()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select * from test");
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.findColumn("Id"));
    }

    @Test
    public void testTableColumnUpperNowFindLowerCaseColumn()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select * from test");
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.findColumn("description"));
    }

    @Test
    public void testTableColumnUpperNowFindUpperCaseColumn()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select * from test");
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.findColumn("DESCRIPTION"));
    }

    @Test
    public void testTableColumnUpperNowFindMixedCaseColumn()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select * from test");
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.findColumn("Description"));
    }

    @Test
    public void testTableColumnMixedNowFindLowerCaseColumn()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select * from test");
        assertTrue(resultSet.next());
        assertEquals(3, resultSet.findColumn("foo"));
    }

    @Test
    public void testTableColumnMixedNowFindUpperCaseColumn()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select * from test");
        assertTrue(resultSet.next());
        assertEquals(3, resultSet.findColumn("FOO"));
    }

    @Test
    public void testTableColumnMixedNowFindMixedCaseColumn()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select * from test");
        assertTrue(resultSet.next());
        assertEquals(3, resultSet.findColumn("fOo"));
    }

    @Test
    public void testSelectWithTableNameAliasNowFindWithoutTableNameAlias()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select t.id from test as t");
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.findColumn("id"));
    }

    /**
     * Can't produce a case where column name contains table name
     * https://www.sqlite.org/c3ref/column_name.html :
     * "If there is no AS clause then the name of the column is unspecified"
     */
    @Test(expected = SQLException.class)
    public void testSelectWithTableNameAliasNowNotFindWithTableNameAlias()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select t.id from test as t");
        assertTrue(resultSet.next());
        resultSet.findColumn("t.id");
    }

    @Test
    public void testSelectWithTableNameNowFindWithoutTableName()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select test.id from test");
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.findColumn("id"));
    }

    @Test(expected = SQLException.class)
    public void testSelectWithTableNameNowNotFindWithTableName()
            throws SQLException {
        ResultSet resultSet = stat.executeQuery("select test.id from test");
        assertTrue(resultSet.next());
        resultSet.findColumn("test.id");
    }

    @Test
    public void testCloseStatement()
        throws SQLException {
        ResultSet resultSet = stat.executeQuery("select test.id from test");

        stat.close();

        assertTrue(stat.isClosed());
        assertTrue(resultSet.isClosed());

        resultSet.close();

        assertTrue(resultSet.isClosed());
    }

    @Test
    public void testCharacterStream() throws SQLException, IOException
    {
        ResultSet resultSet = stat.executeQuery("select test.description from test");

        assertTrue(resultSet.next());

        assertEquals("Buffer smaller than string",
            "description",
            readCharacterStream(resultSet.getCharacterStream(1), 2));
        assertEquals("Buffer larger than string",
            "description",
            readCharacterStream(resultSet.getCharacterStream(1), 1024));

    }

    @Test
    public void testFloatCharacterStream() throws SQLException, IOException
    {
        stat.executeUpdate("insert into test values (4, 1.23456, 'frob')");
        ResultSet resultSet = stat.executeQuery(
            "select test.description from test where id = 4");

        assertTrue(resultSet.next());
        assertEquals(
            "Buffer larger than string representation",
            "1.23456",
            readCharacterStream(resultSet.getCharacterStream(1), 10));
        assertEquals(
            "Buffer smaller than string representation",
            "1.23456",
            readCharacterStream(resultSet.getCharacterStream(1), 2));
    }

    @Test
    public void testLongString() throws SQLException, IOException
    {
        String longString = "Call me Ishmael. Some years ago- never mind how long precisely- having little or no money in my purse, and nothing particular to interest me on shore, I thought I would sail about a little and see the watery part of the world. It is a way I have of driving off the spleen and regulating the circulation. Whenever I find myself growing grim about the mouth; whenever it is a damp, drizzly November in my soul; whenever I find myself involuntarily pausing before coffin warehouses, and bringing up the rear of every funeral I meet; and especially whenever my hypos get such an upper hand of me, that it requires a strong moral principle to prevent me from deliberately stepping into the street, and methodically knocking peoples hats off- then, I account it high time to get to sea as soon as I can. This is my substitute for pistol and ball. With a philosophical flourish Cato throws himself upon his sword; I quietly take to the ship. There is nothing surprising in this. If they but knew it, almost all men in their degree, some time or other, cherish very nearly the same feelings towards the ocean with me.";
        stat.executeUpdate(
            String.format("insert into test values (2, '%s', 'baz')", longString));

        ResultSet resultSet = stat.executeQuery(
            "select test.description from test where id = 2");
        assertTrue(resultSet.next());
        assertEquals("Buffer size smaller than string length",
            longString,
            readCharacterStream(resultSet.getCharacterStream(1),
            1024));

        Reader reader = resultSet.getCharacterStream(1);
        char[] buff = new char[20];
        for (int i = 0; i < 5; i++) {
            buff[i] = 'a';
        }
        reader.read(buff, 5, 15);
        assertEquals(
            "Offset argument",
            "aaaaaCall me Ishmael",
            new String(buff));

    }
    @Test
    public void testNullCharacterStream() throws SQLException
    {
        stat.executeUpdate("insert into test values (3, NULL, 'qux')");
        ResultSet resultSet = stat.executeQuery(
            "select test.description from test where id = 3");

        assertTrue(resultSet.next());
        assertNull(resultSet.getCharacterStream(1));
    }

    @Test
    public void testNonAsciiCharacterStream() throws SQLException, IOException
    {
        String utf8String = "福ずけめ愛海ハナ導復をちて勝惑メオフル張強シトサ無週なせラも管育以ヌ図速かッじつ応一ラミナ街視けうめげ号徴ぎ車始名リレけま青4志ゃ以帯イヤコモ者申章イタ示更ざ准器敗香否にひゅ。主テリ多禁産芸98出づもラら家真チラミ隼誌中ヨレヒル声頭コケヱ庁思ル提98管果らいだ高売とい告広んふず最方ヲ帯求安題ラみやえ。";
        stat.executeUpdate(
            String.format("insert into test values (5, '%s', 'foobar')", utf8String));

        ResultSet resultSet = stat.executeQuery(
            "select test.description from test where id = 5");
        assertTrue(resultSet.next());

        assertEquals("Buffer size smaller than string", utf8String,
            readCharacterStream(resultSet.getCharacterStream(1), 10));
        assertEquals("Buffer size same size as string", utf8String,
            readCharacterStream(resultSet.getCharacterStream(1),
                utf8String.toCharArray().length));
        assertEquals("Buffer size larger than string", utf8String,
            readCharacterStream(resultSet.getCharacterStream(1), 1024));
    }

    private String readCharacterStream(Reader reader, int buffSize) throws IOException
    {
        StringBuilder buffer = new StringBuilder();
        char[] arr = new char[buffSize];
        int read;
        while ((read = reader.read(arr)) != -1) {
            buffer.append(arr, 0, read);
        }
        return buffer.toString();
    }
}
