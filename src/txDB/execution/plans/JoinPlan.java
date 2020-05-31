package txDB.execution.plans;

import txDB.storage.table.Scheme;

import java.util.List;

/**
 * It is a join operator node
 */
public class JoinPlan extends Plan {
    // TODO
    private Scheme scheme;

    public JoinPlan(List<Plan> childrenPlanNodes) {
        super(childrenPlanNodes);
        this.pt = planType.JOIN;
    }
}
