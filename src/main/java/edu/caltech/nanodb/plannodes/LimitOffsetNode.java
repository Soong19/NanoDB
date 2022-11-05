package edu.caltech.nanodb.plannodes;


import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.relations.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;


/**
 * A simple {@code LimitOffsetNode}.
 */
public class LimitOffsetNode extends PlanNode {

    /**
     * A logging object for reporting anything interesting that happens.
     **/
    private static final Logger logger = LogManager.getLogger(LimitOffsetNode.class);

    /**
     * Original limit determines the number of rows returned by the query.
     */
    private final int originLimit;

    /**
     * Original offset skips the offset rows before beginning to return the rows.
     */
    private final int originOffset;

    private int limit; // current limit, 0 for end

    private int offset; // current offset, 0 for no skipping

    public LimitOffsetNode(PlanNode subplan, int limit, int offset) {
        super(subplan);

        if (limit <= 0 && offset <= 0)
            throw new IllegalArgumentException("limit and offset cannot both be zero");

        originLimit = limit;
        originOffset = offset;
    }

    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }

    @Override
    public boolean supportsMarking() {
        return leftChild.supportsMarking();
    }

    @Override
    public boolean requiresLeftMarking() {
        return false;
    }

    @Override
    public boolean requiresRightMarking() {
        return false;
    }

    @Override
    public void prepare() {
        // Need to prepare the left child-node before we can do our own work.
        leftChild.prepare();

        // Use the child's cost and stats unmodified.
        cost = leftChild.getCost();
        stats = leftChild.getStats();

        // initialize
        limit = originLimit;
        offset = originOffset;
    }

    @Override
    public void initialize() {
        super.initialize();
        leftChild.initialize();
    }

    @Override
    public Tuple getNextTuple() {
        if (limit == 0)
            return null;

        while (offset != 0) {
            offset--;
            if (leftChild.getNextTuple() == null)
                offset = 0;
        }

        limit--;
        return leftChild.getNextTuple();
    }

    @Override
    public void markCurrentPosition() throws UnsupportedOperationException {
        leftChild.markCurrentPosition();
    }

    @Override
    public void resetToLastMark() {
        leftChild.resetToLastMark();
    }

    @Override
    public void cleanUp() {
        leftChild.cleanUp();
    }

    @Override
    public String toString() {
        return "LimitOffset[limit=" + originLimit + "offset" + originOffset + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LimitOffsetNode) {
            LimitOffsetNode other = (LimitOffsetNode) obj;
            return originLimit == other.originLimit &&
                other.originOffset == originOffset &&
                leftChild.equals(other.leftChild);
        }

        return false;
    }

    @Override
    public int hashCode() {
        int hash = 17;
        hash = 13 * hash + originLimit;
        hash = 17 * hash + originOffset;
        hash = 37 * hash + leftChild.hashCode();
        return hash;
    }
}
