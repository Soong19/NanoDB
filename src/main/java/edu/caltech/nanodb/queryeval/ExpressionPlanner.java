package edu.caltech.nanodb.queryeval;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.queryast.SelectClause;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ExpressionPlanner is a class that traverses an entire expression,
 * planning any subqueries that appear in the expression, and ensure
 * <tt>ORDER BY</tt> & <tt>GROUP BY</tt> clauses have no subquery.
 * <p>
 * The main task of this class is: build an environment chain so that
 * subquery can walk bottom-up to get tuple from parent environment.
 * <p>
 * To support correlated subquery, NanoDB uses an environment chain.
 * That is, a subquery will add its parent (and parent's parent) env
 * to its own env. Therefore, when an expression is evaluated in this
 * node and a column cannot be found, it will pass the reference to
 * its parent env until the column reference can be resolved. When
 * retrieving a tuple from a node, it also stores tuple in its
 * environment. Therefore, when subquery acquires a tuple, it can
 * easily get it from the parent environment.
 */
public class ExpressionPlanner implements ExpressionProcessor {

    // An instantiated planner for constructing a subquery tree.
    private final Planner planner;

    // Major select clause to handle with.
    private final SelectClause selClause;

    // Enclosing selects inherited from parent planner.
    private final List<SelectClause> enclosingSels = null;

    // The expression-planner's environment.
    private Environment env = new Environment();

    // Record subqueries in an expression.
    private List<SubqueryOperator> subqueryOperators = new ArrayList<>();

    ExpressionPlanner(SelectClause selectClause, Planner joinPlanner, List<SelectClause> enclosingSelects) {
        planner = joinPlanner;
        selClause = selectClause;

        /*=== Enclosing selects => have no impact *===*/
        // for child subquery to find outer query's columns (a.k.a., current root node)
        // enclosingSels = new ArrayList<>();
        // if (enclosingSelects != null) enclosingSels.addAll(enclosingSelects);
        // enclosingSels.add(selectClause);

        validateGroupBy(selectClause.getGroupByExprs());
        validateOrderBy(selectClause.getOrderByExprs());
    }

    /**
     * Walk through all the select values, generate plan node for subqueries.
     * <p>
     * Possible subqueries: {@link SubqueryOperator}
     */
    public boolean scanSelVals() {
        var selVals = selClause.getSelectValues();
        assert !selVals.isEmpty();

        AtomicBoolean doMakePlan = new AtomicBoolean(false);
        selVals.stream()
            .filter(sv -> sv.isExpression())
            .forEach(sv -> doMakePlan.set(makePlan(sv.getExpression()) || doMakePlan.get()));
        return doMakePlan.get();
    }

    /**
     * Walk through <tt>WHERE</tt>-expression, generate plan node for subqueries.
     * <p>
     * Possible subqueries: {@link SubqueryOperator}, {@link InSubqueryOperator}, {@link ExistsOperator}
     */
    public boolean scanWhere() {
        return makePlan(selClause.getWhereExpr());
    }

    /**
     * Walk through <tt>HAVING</tt>-expression, generate plan node for subqueries.
     * <p>
     * Possible subqueries: {@link SubqueryOperator}, {@link InSubqueryOperator}, {@link ExistsOperator}
     */
    public boolean scanHaving() {
        return makePlan(selClause.getHavingExpr());
    }

    /**
     * Generate a plan node for a subquery expression.
     *
     * @return true if make a plan for expr, false otherwise
     */
    private boolean makePlan(Expression expr) {
        if (expr == null) {
            return false;
        }
        // walk through the expr to get all the subqueries
        expr.traverse(this);
        // Generate plan for each subquery
        subqueryOperators.stream().forEach(sq -> {
            var plan = planner.makePlan(sq.getSubquery(), enclosingSels);
            plan.addParentEnvironmentToPlanTree(env);
            sq.setSubqueryPlan(plan);
        });
        // clear the subqueries for next invocation
        subqueryOperators.clear();
        return true;
    }

    /**
     * Ensure no subquery in <tt>GROUP BY</tt> clause.
     */
    private void validateGroupBy(List<Expression> expressions) {
        if (expressions.stream()
            .filter(e -> e instanceof SubqueryOperator)
            .count() > 0) {
            throw new UnsupportedOperationException("Cannot use subquery in GROUP-BY-clause");
        }
    }

    /**
     * Ensure no subquery in <tt>ORDER BY</tt> clause.
     */
    private void validateOrderBy(List<OrderByExpression> expressions) {
        if (expressions.stream()
            .filter(e -> e.getExpression() instanceof SubqueryOperator)
            .count() > 0) {
            throw new UnsupportedOperationException("Cannot use subquery in ORDER-BY-clause");
        }
    }

    /**
     * Return current environment and reset environment for next invocation.
     * <p>
     * NOTE: This method also reset environment.
     *
     * @return environment just computed
     */
    public Environment getEnvironment() {
        var envCpy = env;
        env = new Environment();
        return envCpy;
    }

    /*=== Processor APIs ===*/

    /**
     * Check whether the expression is {@link SubqueryOperator} or not.
     * If is, need to generate a query plan for it. Otherwise, do nothing.
     */
    @Override
    public void enter(Expression node) {
        if (node instanceof SubqueryOperator) {
            subqueryOperators.add((SubqueryOperator) node);
        }
    }

    /**
     * Do nothing.
     */
    @Override
    public Expression leave(Expression node) {
        return node;
    }
}
