package edu.caltech.nanodb.queryeval;


import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
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
public class SimplePlanner extends AbstractPlannerImpl {

    /**
     * A logging object for reporting anything interesting that happens.
     */
    private static final Logger logger = LogManager.getLogger(SimplePlanner.class);

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
            logger.debug("Subquery");
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
            validateExpression(whereClause, "WHERE");
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
    public PlanNode makeSelect(FromClause fromClause) {
        PlanNode node = null;
        logger.debug("From: " + fromClause);
        switch (fromClause.getClauseType()) {
            case BASE_TABLE:
                node = makeSimpleSelect(fromClause.getTableName(), null, null);
                break;
            case JOIN_EXPR:
                // recursive process: compute left & right, then combine them together
                var l_node = makeSelect(fromClause.getLeftChild());
                var r_node = makeSelect(fromClause.getRightChild());

                validateExpression(fromClause.getComputedJoinExpr(), "ON");
                node = new NestedLoopJoinNode(l_node, r_node, fromClause.getJoinType(), fromClause.getComputedJoinExpr());
                break;
            case SELECT_SUBQUERY:
                node = makePlan(fromClause.getSelectClause(), null);
                break;
            case TABLE_FUNCTION:
                throw new UnsupportedOperationException("Not implemented:  Table Function");
            default:
                assert false;
        }
        // rename node for AS; SELECT tbl2.a FROM tbl1 AS tbl2
        if (fromClause.isRenamed()) {
            node = new RenameNode(node, fromClause.getResultName());
        }

        return node;
    }
}
