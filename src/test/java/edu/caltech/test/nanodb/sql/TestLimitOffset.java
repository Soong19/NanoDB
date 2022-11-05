package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


/**
 * This class exercises the database with some simple <tt>LIMIT OFFSET</tt>
 * statements against a single table, to see if simple selects and
 * predicates work properly.
 */
@Test(groups = {"sql", "hw2"})
public class TestLimitOffset extends SqlTestCase {

    public TestLimitOffset() {
        super("setup_testLimitOffset");
    }


    /**
     * This test performs a normal <tt>LIMIT OFFSET</tt> statement, to see if
     * the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testNormalLimitOffset() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(1, 10),
            new TupleLiteral(2, 20),
        };

        CommandResult result = server.doCommand(
            "SELECT * FROM test_limit_offset LIMIT 2 OFFSET 1", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs a <tt>LIMIT OFFSET</tt> statement with limit=0, to see
     * if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testZeroLimit() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result = server.doCommand(
            "SELECT * FROM test_limit_offset LIMIT 0 OFFSET 1", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs a <tt>LIMIT OFFSET</tt> statement with offset=0, to
     * see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testZeroOffset() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(0, null),
            new TupleLiteral(1, 10),
            new TupleLiteral(2, 20),
        };

        CommandResult result = server.doCommand(
            "SELECT * FROM test_limit_offset LIMIT 3 OFFSET 0", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs a <tt>LIMIT OFFSET</tt> statement with both offset and
     * limit equal 0, to see if the query produces the expected results.
     * <p>
     * Equivalent to no <tt>LIMIT OFFSET</tt>.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testZeroLimitOffset() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(0, null),
            new TupleLiteral(1, 10),
            new TupleLiteral(2, 20),
            new TupleLiteral(3, 30),
            new TupleLiteral(4, null)
        };

        CommandResult result = server.doCommand(
            "SELECT * FROM test_limit_offset LIMIT 0 OFFSET 0", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs a <tt>LIMIT OFFSET</tt> statement with overflow limit,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testOverflowLimit() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(1, 10),
            new TupleLiteral(2, 20),
            new TupleLiteral(3, 30),
            new TupleLiteral(4, null)
        };

        CommandResult result = server.doCommand(
            "SELECT * FROM test_limit_offset LIMIT 100 OFFSET 1", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs a <tt>LIMIT OFFSET</tt> statement with overflow offset,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testOverflowOffset() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result = server.doCommand(
            "SELECT * FROM test_limit_offset LIMIT 3 OFFSET 100", true);
        assert checkUnorderedResults(expected, result);
    }
}
