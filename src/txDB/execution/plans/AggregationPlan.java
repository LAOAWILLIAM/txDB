package txDB.execution.plans;

import java.util.List;

public class AggregationPlan extends Plan {
    public AggregationPlan(List<Plan> childrenPlanNodes) {
        super(childrenPlanNodes);
        this.pt = planType.AGGREGATION;
    }
}
