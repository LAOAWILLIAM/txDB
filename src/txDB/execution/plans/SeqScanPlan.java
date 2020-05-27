package txDB.execution.plans;

import java.util.List;

/**
 * It is a sequential scan operator node
 */
public class SeqScanPlan extends Plan {
    // TODO
    private final String relationName;

    public SeqScanPlan(List<Plan> childrenPlanNodes, String relationName) {
        super(childrenPlanNodes);
        this.relationName = relationName;
        this.pt = planType.SEQSCAN;
    }

    public String getRelationName() {
        return relationName;
    }
}
