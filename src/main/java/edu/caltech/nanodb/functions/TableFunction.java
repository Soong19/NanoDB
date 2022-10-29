package edu.caltech.nanodb.functions;


import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.queryeval.ColumnStats;
import edu.caltech.nanodb.queryeval.PlanCost;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * This is the base-class for all table-returning functions.
 */
public abstract class TableFunction extends Function {

    /**
     * If the results are ordered in some way, this method returns a collection
     * of expressions specifying what columns or expressions the results are
     * ordered by.  If the results are not ordered then this method may return
     * either an empty list or a <tt>null</tt> value.
     * <p>
     * When this method returns a list of ordering expressions, the order of the
     * expressions themselves also matters.  The entire set of results will be
     * ordered by the first expression; rows with the same value for the first
     * expression will be ordered by the second expression; etc.
     *
     * @return If the plan node produces ordered results, this will be a list
     * of objects specifying the ordering.  If the node doesn't produce
     * ordered results then the return-value will either be an empty
     * list or it will be <tt>null</tt>.
     */
    public abstract List<OrderByExpression> resultsOrderedBy();


    /**
     * This method is responsible for computing critical details about the plan
     * node, such as the schema of the results that are produced, the estimated
     * cost of evaluating the plan node (and its children), and statistics
     * describing the results produced by the plan node.
     */
    public abstract void prepare();


    /**
     * Returns the schema of the results that this node produces.  Some nodes
     * such as Select will not change the input schema but others, such as
     * Project, Rename, and ThetaJoin, must change it.
     * <p>
     * The schema is not computed until the {@link #prepare} method is called;
     * until that point, this method will return <tt>null</tt>.
     *
     * @return the schema produced by this plan-node.
     */
    public abstract Schema getSchema();


    /**
     * Returns the estimated cost of this plan node's operation.  The estimate
     * depends on which algorithm the node uses and the data it is working with.
     * <p>
     * The cost is not computed until the {@link #prepare} method is called;
     * until that point, this method will return <tt>null</tt>.
     *
     * @return an object containing various cost measures such as the worst-case
     * number of disk accesses, the number of tuples produced, etc.
     */
    public abstract PlanCost getCost();


    /**
     * Returns statistics (possibly estimated) describing the results that this
     * plan node will produce.  Estimating statistics for output results is a
     * very imprecise task, to say the least.
     * <p>
     * These statistics are not computed until the {@link #prepare} method is
     * called; until that point, this method will return <tt>null</tt>.
     *
     * @return statistics describing the results that will be produced by this
     * plan-node.
     */
    public abstract ArrayList<ColumnStats> getStats();


    /**
     * Does any initialization the node might need.  This could include
     * resetting state variables or starting the node over from the beginning.
     */
    public abstract void initialize();


    /**
     * Gets the next tuple that fulfills the conditions for this plan node.
     * If the node has a child, it should call getNextTuple() on the child.
     * If the node is a leaf, the tuple comes from some external source such
     * as a table file, the network, etc.
     *
     * @return the next tuple to be generated by this plan, or <tt>null</tt>
     * if the plan has finished generating plan nodes.
     */
    public abstract Tuple getNextTuple();


    /**
     * Perform any necessary clean up tasks. This should probably be called
     * when we are done with this plan node.
     */
    public abstract void cleanUp();
}