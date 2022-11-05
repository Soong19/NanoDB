package edu.caltech.nanodb.queryeval;


import edu.caltech.nanodb.expressions.AggregationProcessor;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;


/**
 * This class generates execution plans for very simple SQL
 * <tt>SELECT * FROM tbl [WHERE P]</tt> queries.  The primary responsibility
 * is to generate plans for SQL <tt>SELECT</tt> statements, but
 * <tt>UPDATE</tt> and <tt>DELETE</tt> expressions will also use this class
 * to generate simple plans to identify the tuples to update or delete.
 */
public class SimplePlanner implements Planner {

    /**
     * A logging object for reporting anything interesting that happens.
     */
    private static final Logger logger = LogManager.getLogger(SimplePlanner.class);


    /**
     * The storage manager used during query planning.
     */
    protected StorageManager storageManager;


    /**
     * Sets the server to be used during query planning.
     */
    public void setStorageManager(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.storageManager = storageManager;
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     * @return a plan tree for executing the specified query
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
                             List<SelectClause> enclosingSelects) {
        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                "Not implemented:  enclosing queries");
        }

        PlanNode plan; // root node

        var fromClause = selClause.getFromClause();
        var whereClause = selClause.getWhereExpr();

        // 1. FromClause: generate plan node
        if (fromClause == null) {
            logger.debug("From: 0 table");
            // example: SELECT 2 + 3 AS five;
            plan = new ProjectNode(selClause.getSelectValues());
        } else {
            // non-Join: no predicate
            // Join: ON or USING is defined in fromClause
            plan = makeSelect(fromClause);
        }

        // 2. WhereClause: add where clause
        if (whereClause != null) {
            logger.debug("Where: " + whereClause);
            PlanUtils.validateExpression(whereClause, "WHERE");
            plan = PlanUtils.addPredicateToPlan(plan, whereClause);
        }

        // 3. Grouping & Aggregation: handle_grouping and aggregation
        plan = handleGroupAggregate(plan, selClause);

        // 4. Order By: add order-by clause
        if (!selClause.getOrderByExprs().isEmpty()) {
            logger.debug("Order By: " + selClause.getOrderByExprs());
            plan = new SortNode(plan, selClause.getOrderByExprs());
        }

        // 5. Project: add a filter for columns
        if (!selClause.isTrivialProject()) {
            plan = new ProjectNode(plan, selClause.getSelectValues());
        }

        // 6. Limit & Offset: add a limiter for results
        if (selClause.getLimit() != 0 || selClause.getOffset() != 0)
            plan = new LimitOffsetNode(plan, selClause.getLimit(), selClause.getOffset());

        plan.prepare();
        return plan;
    }

    /**
     * Construct a select node, which involve 1 or more tables.
     * Support base-table, join, subquery.
     *
     * @param fromClause The from-clause contains 0 or 1 or more tables.
     * @return A new plan-node for evaluating the select operation.
     */
    private PlanNode makeSelect(FromClause fromClause) {
        PlanNode node = null;
        logger.debug("From: " + fromClause);
        switch (fromClause.getClauseType()) {
            case BASE_TABLE:
                node = makeSimpleSelect(fromClause.getTableName(), null, null);
                break;
            case JOIN_EXPR:
                node = PlanUtils.computeJoin(fromClause, storageManager);
                break;
            case SELECT_SUBQUERY:
            case TABLE_FUNCTION:
                throw new UnsupportedOperationException("Not implemented:  Subquery or Table Function");
            default:
                assert false;
        }
        return node;
    }

    /**
     * Process on <tt>GROUP BY</tt>, <tt>HAVING</tt> and {@code Aggregation}.
     * <p>
     * First, map every aggregate to an {@code ColumnValue} with auto-generated
     * name.
     * Second, initialize {@link HashedGroupAggregateNode} with mapping and
     * aggregation calls.
     * Third, add an {@link SimpleFilterNode} if necessary.
     *
     * @param plan      child node to getNextTuple
     * @param selClause select clause to be checked
     * @return plan node processed <tt>GROUP BY</tt> and {@code Aggregation}
     */
    private PlanNode handleGroupAggregate(PlanNode plan, SelectClause selClause) {
        logger.debug("Group By: " + selClause.getHavingExpr());

        var processor = new AggregationProcessor();

        var selectValues = selClause.getSelectValues();
        var groupByExprs = selClause.getGroupByExprs();
        var havingExpr = selClause.getHavingExpr();

        // Process on SELECT
        for (var sv : selectValues) {
            // Skip select-values that aren't expressions
            if (!sv.isExpression())
                continue;
            Expression e = sv.getExpression().traverse(processor);
            sv.setExpression(e);
        }

        // Process on HAVING
        if (havingExpr != null)
            havingExpr = havingExpr.traverse(processor);

        // Process on GROUP BY
        PlanUtils.validateExpression(groupByExprs, "GROUP BY");
        if (!groupByExprs.isEmpty() || !processor.getAggregates().isEmpty()) {
                plan = new HashedGroupAggregateNode(plan, groupByExprs, processor.getAggregates());
            if (havingExpr != null)
                plan = PlanUtils.addPredicateToPlan(plan, havingExpr);
        }

        return plan;
    }


    /**
     * Construct a simple select node, which just read from a table with an
     * optional predicate.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     * @param predicate An optional selection predicate, or {@code null} if
     *                  no filtering is desired.
     * @return A new plan-node for evaluating the select operation.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
                                       List<SelectClause> enclosingSelects) {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo =
            storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        return new FileScanNode(tableInfo, predicate);
    }
}
