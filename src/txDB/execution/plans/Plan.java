package txDB.execution.plans;

import java.util.ArrayList;
import java.util.List;

/**
 * I use "Iterator model" here, each plan is a node
 * It is a father class of plan node
 */
public class Plan {
    // TODO
    protected List<Plan> childrenPlanNodes;
    public enum planType {SEQSCAN, INSERT, JOIN, AGGREGATION, PREDEVAL}
    protected planType pt;

    public Plan(List<Plan> childrenPlanNodes) {
        this.childrenPlanNodes = new ArrayList<>(childrenPlanNodes);
    }

    public List<Plan> getChildrenPlanNodes() {
        return childrenPlanNodes;
    }

    public planType getPlanType() {
        return pt;
    }
}
