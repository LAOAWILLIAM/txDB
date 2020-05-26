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

    public Plan(List<Plan> childrenPlanNodes) {
        this.childrenPlanNodes = new ArrayList<>(childrenPlanNodes);
    }

    public List<Plan> getChildrenPlanNodes() {
        return childrenPlanNodes;
    }
}
