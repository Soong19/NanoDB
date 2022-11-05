package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.queryeval.InvalidSQLException;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * This {@code ExpressionProcessor} implementation traverses an expression
 * to identify and extract aggregate functions from an expression, and to report
 * errors like nested aggregate function calls.<br/>
 * And also, it generates and maps each one to a placeholder variable {@link
 * ColumnValue} with an auto-generated name.
 * </p>
 *
 * <p>
 * This class {@code AggregationProcessor} is used by <tt>SELECT</tt> and
 * <tt>HAVING</tt>. {@code AggregationProcessor} validates and generates
 * placeholder variables for aggregation expressions so that the <tt>GROUP
 * BY</tt> plan-node can be initialized correctly.
 * </p>
 */
public class AggregationProcessor implements ExpressionProcessor {

    /**
     * Mark whether the expression has been aggregated.
     *
     * @apiNote How does this work? Parent expression would {@link
     * #enter(Expression)} before each child to set {@code hasAggregate}. When
     * children access {@link #enter(Expression)}, if it is an instance of
     * {@code FunctionCall}, then throw an exception. They are using the same
     * processor.
     */
    private boolean hasAggregate = false;

    /**
     * Mapping from an auto-generated name to an {@link AggregateFunction}.
     */
    private final Map<String, FunctionCall> aggregates = new HashMap<>();

    /**
     * Check whether there is a nested-aggregation call. If there is, throw an
     * exception; otherwise, do nothing.
     *
     * @param node the {@code Expression} node being entered
     */
    @Override
    public void enter(Expression node) {
        if (node instanceof FunctionCall) {
            var func = ((FunctionCall) node).getFunction();
            if (func instanceof AggregateFunction) {
                if (hasAggregate)
                    throw new InvalidSQLException("nested aggregate function calls");
                else
                    hasAggregate = true;
            }
        }
    }

    /**
     * Replace an aggregate function call with a column-reference {@link ColumnValue}.
     *
     * @param node the {@code Expression} node being left
     * @return a placeholder variable {@link ColumnValue} with an auto-generated
     * name
     */
    @Override
    public Expression leave(Expression node) {
        if (node instanceof FunctionCall) {
            var call = (FunctionCall) node;
            var func = (call).getFunction();
            if (func instanceof AggregateFunction) {
                hasAggregate = false;

                var columnName = node.toString();
                aggregates.put(columnName, call);

                return new ColumnValue(new ColumnName(columnName));
            }
        }
        return node;
    }

    public Map<String, FunctionCall> getAggregates() {
        return aggregates;
    }
}
