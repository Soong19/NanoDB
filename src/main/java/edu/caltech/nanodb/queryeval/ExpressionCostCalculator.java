package edu.caltech.nanodb.queryeval;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionProcessor;
import edu.caltech.nanodb.expressions.SubqueryOperator;

/**
 * {@link ExpressionCostCalculator} class is responsible for calculate
 * the cost of an {@link Expression}.
 */
public class ExpressionCostCalculator implements ExpressionProcessor {
    private PlanCost cost = new PlanCost(0, 0, 0, 0, 0);

    /**
     * Increment the CPU cost by 1 every time you enter a new expression.
     * When encounter a subquery, increment CPU cost by the plan's cost.
     */
    @Override
    public void enter(Expression node) {
        if (node instanceof SubqueryOperator) {
            var subCost = ((SubqueryOperator) node).getSubqueryPlan().getCost().cpuCost;
            cost.cpuCost += subCost > 0 ? subCost : 1;
        } else {
            cost.cpuCost++;
        }
    }

    /**
     * Do nothing.
     */
    @Override
    public Expression leave(Expression node) {
        return node;
    }

    public PlanCost getCost() {
        return cost;
    }
}
