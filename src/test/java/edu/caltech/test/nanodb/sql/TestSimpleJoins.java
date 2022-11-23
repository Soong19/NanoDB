package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


/**
 * This class exercises the database with some simple <tt>JOIN</tt> statements
 * against two tables, to see whether the joins work properly.
 */
@Test(groups = {"sql", "hw2"})
public class TestSimpleJoins extends SqlTestCase {

    public TestSimpleJoins() {
        /*
         * test_join_left AND test_join_mid AND test_join_right: three normal tables ON a=c=e
         * test_join_empty_left AND test_join_empty_right
         * test_join_one_row AND test_join_multiple_row: two tables ON a=d
         */
        super("setup_testJoin");
    }


    /**
     * This test performs a simple <tt>INNER JOIN</tt> statement against two
     * normal tables, to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testNormalInnerJoin() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(1, 10, 1, "a"),
            new TupleLiteral(1, 20, 1, "a"),
            new TupleLiteral(3, null, 3, "b"),
            new TupleLiteral(5, 40, 5, "c")
        };

        CommandResult result = server.doCommand(
            "SELECT a, b, c, d FROM test_join_left INNER JOIN test_join_right ON a = c", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs a simple <tt>LEFT OUTER JOIN</tt> statement against
     * two normal tables, to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testNormalLeftOuterJoin() throws Throwable {
        TupleLiteral[] expected = null;
        try {
            expected = new TupleLiteral[]{
                new TupleLiteral(0, null, null),
                new TupleLiteral(0, null, null),
                new TupleLiteral(0, null, null),
                new TupleLiteral(0, null, null),
                new TupleLiteral(0, null, null),
                new TupleLiteral(1, 10, "a"),
                new TupleLiteral(1, 10, null),
                new TupleLiteral(1, 10, null),
                new TupleLiteral(1, 10, null),
                new TupleLiteral(1, 10, null),
                new TupleLiteral(1, 20, "a"),
                new TupleLiteral(1, 20, null),
                new TupleLiteral(1, 20, null),
                new TupleLiteral(1, 20, null),
                new TupleLiteral(1, 20, null),
                new TupleLiteral(2, 30, null),
                new TupleLiteral(2, 30, null),
                new TupleLiteral(2, 30, null),
                new TupleLiteral(2, 30, null),
                new TupleLiteral(2, 30, null),
                new TupleLiteral(3, null, "b"),
                new TupleLiteral(3, null, null),
                new TupleLiteral(3, null, null),
                new TupleLiteral(3, null, null),
                new TupleLiteral(3, null, null),
                new TupleLiteral(5, 40, "c"),
                new TupleLiteral(5, 40, null),
                new TupleLiteral(5, 40, null),
                new TupleLiteral(5, 40, null),
                new TupleLiteral(5, 40, null),
                new TupleLiteral(8, 50, null),
                new TupleLiteral(8, 50, null),
                new TupleLiteral(8, 50, null),
                new TupleLiteral(8, 50, null),
                new TupleLiteral(8, 50, null),
                new TupleLiteral(13, 60, null),
                new TupleLiteral(13, 60, null),
                new TupleLiteral(13, 60, null),
                new TupleLiteral(13, 60, null),
                new TupleLiteral(13, 60, null),
            };
        } finally {
            CommandResult result = server.doCommand(
                "SELECT a, b, d FROM test_join_left LEFT OUTER JOIN test_join_right ON a = c", true);
            assert checkUnorderedResults(expected, result);
        }
    }

    /**
     * This test performs a simple <tt>RIGHT OUTER JOIN</tt> statement against
     * two normal tables, to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testNormalRightOuterJoin() throws Throwable {
        TupleLiteral[] expected = null;
        try {
            expected = new TupleLiteral[]{
                new TupleLiteral(1, 10, "a"),
                new TupleLiteral(1, 20, "a"),
                new TupleLiteral(1, null, "a"),
                new TupleLiteral(1, null, "a"),
                new TupleLiteral(1, null, "a"),
                new TupleLiteral(1, null, "a"),
                new TupleLiteral(1, null, "a"),
                new TupleLiteral(1, null, "a"),
                new TupleLiteral(3, null, "b"),
                new TupleLiteral(3, null, "b"),
                new TupleLiteral(3, null, "b"),
                new TupleLiteral(3, null, "b"),
                new TupleLiteral(3, null, "b"),
                new TupleLiteral(3, null, "b"),
                new TupleLiteral(3, null, "b"),
                new TupleLiteral(3, null, "b"),
                new TupleLiteral(5, 40, "c"),
                new TupleLiteral(5, null, "c"),
                new TupleLiteral(5, null, "c"),
                new TupleLiteral(5, null, "c"),
                new TupleLiteral(5, null, "c"),
                new TupleLiteral(5, null, "c"),
                new TupleLiteral(5, null, "c"),
                new TupleLiteral(5, null, "c"),
                new TupleLiteral(7, null, null),
                new TupleLiteral(7, null, null),
                new TupleLiteral(7, null, null),
                new TupleLiteral(7, null, null),
                new TupleLiteral(7, null, null),
                new TupleLiteral(7, null, null),
                new TupleLiteral(7, null, null),
                new TupleLiteral(7, null, null),
                new TupleLiteral(9, null, "e"),
                new TupleLiteral(9, null, "e"),
                new TupleLiteral(9, null, "e"),
                new TupleLiteral(9, null, "e"),
                new TupleLiteral(9, null, "e"),
                new TupleLiteral(9, null, "e"),
                new TupleLiteral(9, null, "e"),
                new TupleLiteral(9, null, "e"),
            };
        } finally {
            CommandResult result = server.doCommand(
                "SELECT c, b, d FROM test_join_left RIGHT OUTER JOIN test_join_right ON a = c", true);
            assert checkUnorderedResults(expected, result);
        }
    }

    /**
     * This test performs a simple <tt>LEFT OUTER JOIN</tt>, <tt>RIGHT OUTER
     * JOIN</tt>, <tt>INNER JOIN</tt> statement against an empty table (left)
     * and a normal table (right), to see if the query produces the expected
     * results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testEmptyLeftOuterJoin() throws Throwable {
        TupleLiteral[] expected = null;
        try {
            expected = new TupleLiteral[]{
            };
        } finally {
            CommandResult result0 = server.doCommand(
                "SELECT * FROM test_join_empty_left INNER JOIN test_join_right ON e = c", true);
            assert checkUnorderedResults(expected, result0);

            CommandResult result1 = server.doCommand(
                "SELECT * FROM test_join_empty_left LEFT OUTER JOIN test_join_right ON e = c", true);
            assert checkUnorderedResults(expected, result1);

            CommandResult result2 = server.doCommand(
                "SELECT * FROM test_join_empty_left RIGHT OUTER JOIN test_join_right ON e = c", true);
            assert checkUnorderedResults(expected, result2);
        }
    }

    /**
     * This test performs a simple <tt>LEFT OUTER JOIN</tt>, <tt>RIGHT OUTER
     * JOIN</tt>, <tt>INNER JOIN</tt> statement against an empty table (right)
     * and a normal table (left), to see if the query produces the expected
     * results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testEmptyRightOuterJoin() throws Throwable {
        TupleLiteral[] expected = null;
        try {
            expected = new TupleLiteral[]{
            };
        } finally {
            CommandResult result0 = server.doCommand(
                "SELECT * FROM test_join_left INNER JOIN test_join_empty_right ON a = q", true);
            assert checkUnorderedResults(expected, result0);

            CommandResult result1 = server.doCommand(
                "SELECT * FROM test_join_left LEFT OUTER JOIN test_join_empty_right ON a = q", true);
            assert checkUnorderedResults(expected, result1);

            CommandResult result2 = server.doCommand(
                "SELECT * FROM test_join_left RIGHT OUTER JOIN test_join_empty_right ON a = q", true);
            assert checkUnorderedResults(expected, result2);
        }
    }

    /**
     * This test performs a simple <tt>INNER JOIN</tt> statement against two
     * empty tables, to see if the query produces the expected results.
     * This test performs a simple <tt>LEFT OUTER JOIN</tt>, <tt>RIGHT OUTER
     * JOIN</tt>, <tt>INNER JOIN</tt> statement against two empty tables, to see
     * if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testEmptyInnerJoin() throws Throwable {
        TupleLiteral[] expected = null;
        try {
            expected = new TupleLiteral[]{
            };
        } finally {
            CommandResult result0 = server.doCommand(
                "SELECT * FROM test_join_empty_left INNER JOIN test_join_empty_right ON e = q", true);
            assert checkUnorderedResults(expected, result0);

            CommandResult result1 = server.doCommand(
                "SELECT * FROM test_join_empty_left LEFT OUTER JOIN test_join_empty_right ON e = q", true);
            assert checkUnorderedResults(expected, result1);

            CommandResult result2 = server.doCommand(
                "SELECT * FROM test_join_empty_left RIGHT OUTER JOIN test_join_empty_right ON e = q", true);
            assert checkUnorderedResults(expected, result2);
        }
    }

    /**
     * This test performs a simple <tt>INNER JOIN</tt> statement against a table
     * with one row (left) and a table with multiple rows (right), to see if the
     * query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testOneRowLeft() throws Throwable {
        TupleLiteral[] expected = null;
        try {
            expected = new TupleLiteral[]{
                new TupleLiteral(1, "a", 1, 1, 2),
            };
        } finally {
            CommandResult result = server.doCommand(
                "SELECT a, b, c, d, e FROM test_join_one_row INNER JOIN test_join_multiple_row ON a = c", true);
            assert checkUnorderedResults(expected, result);
        }
    }

    /**
     * This test performs a simple <tt>INNER JOIN</tt> statement against a table
     * with one row (right) and a table with multiple rows (left), to see if the
     * query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testOneRowRight() throws Throwable {
        TupleLiteral[] expected = null;
        try {
            expected = new TupleLiteral[]{
                new TupleLiteral(1, 1, 2, 1, "a"),
            };
        } finally {
            CommandResult result = server.doCommand(
                "SELECT c, d, e, a, b FROM test_join_multiple_row INNER JOIN test_join_one_row ON a = c", true);
            assert checkUnorderedResults(expected, result);
        }
    }

    /**
     * This test performs a simple <tt>INNER JOIN</tt> (<tt>,</tt> and
     * <tt>WHERE</tt> statement against two normal tables, to see if the query
     * produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoinWhere() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(1, 10, 1, "a"),
            new TupleLiteral(1, 20, 1, "a"),
            new TupleLiteral(3, null, 3, "b"),
            new TupleLiteral(5, 40, 5, "c")
        };

        CommandResult result = server.doCommand(
            "SELECT a, b, c, d FROM test_join_left, test_join_right WHERE a = c", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs a simple <tt>INNER JOIN</tt> (<tt>,</tt> and
     * <tt>WHERE</tt> statement against three normal tables, to see if the query
     * produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testThreeInnerJoin() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(1, 10, 1, "a", 1, 100),
            new TupleLiteral(1, 20, 1, "a", 1, 100),
            new TupleLiteral(5, 40, 5, "c", 5, 200)
        };

        CommandResult result1 = server.doCommand(
            "SELECT a, b, c, d, e, f FROM test_join_left INNER JOIN test_join_right ON a = c INNER JOIN test_join_mid ON a = e", true);
        assert checkUnorderedResults(expected, result1);

        CommandResult result2 = server.doCommand(
            "SELECT a, b, c, d, e, f FROM test_join_left, test_join_right, test_join_mid WHERE a = e AND a = c", true);
        assert checkUnorderedResults(expected, result2);
    }
}
