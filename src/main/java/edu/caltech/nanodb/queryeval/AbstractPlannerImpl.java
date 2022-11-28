package edu.caltech.nanodb.queryeval;

import edu.caltech.nanodb.expressions.AggregationProcessor;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * This abstract class is used for reuse code easily. It contains some same
 * functionalities that appear in multiple classes. Such as,
 * {@link #handleGroupAggregate}, {@link #makeSimpleSelect} and
 * {@link #validateExpression(Expression, String)},
 */
public abstract class AbstractPlannerImpl implements Planner {
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
     * Ensure <tt>WHERE</tt>, <tt>ON</tt> contain no aggregates.
     *
     * @param expression the expression to be checked
     * @param clauseName clause name for more specific exception (it's better to
     *                   use a record class to record)
     */
    protected static void validateExpression(Expression expression, String clauseName) {
        if (expression instanceof FunctionCall) {
            var func = ((FunctionCall) expression).getFunction();
            if (func instanceof AggregateFunction) {
                throw new InvalidSQLException(
                    "clause " + clauseName.toUpperCase() + " cannot contain aggregates");
            }
        }
    }

    /**
     * Ensure <tt>GROUP BY</tt> contains no aggregates.
     *
     * @param expressions the expressions to be checked
     * @param clauseName  clause name for more specific exception (it's better to
     *                    use a record class to record)
     */
    protected static void validateExpression(List<Expression> expressions, String clauseName) {
        for (var expression : expressions) {
            validateExpression(expression, clauseName);
        }
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
    protected PlanNode handleGroupAggregate(PlanNode plan, SelectClause selClause) {
        logger.debug("Group By: " + selClause.getGroupByExprs());

        var processor = new AggregationProcessor();

        var selectValues = selClause.getSelectValues();
        var groupByExprs = selClause.getGroupByExprs();
        var havingExpr = selClause.getHavingExpr();

        // Process on SELECT
        for (var sv : selectValues) {
            // Skip select-values that aren't expressions
            if (!sv.isExpression())
                continue;
            var e = sv.getExpression().traverse(processor);
            sv.setExpression(e);
        }

        // Process on HAVING
        if (havingExpr != null)
            havingExpr = havingExpr.traverse(processor);

        // Process on GROUP BY
        validateExpression(groupByExprs, "GROUP BY");
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
    public SelectNode makeSimpleSelect(String tableName, Expression predicate, List<SelectClause> enclosingSelects) {
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
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        return new FileScanNode(tableInfo, predicate);
    }
}
