package txDB.execution.plans;

import txDB.storage.table.Scheme;

import java.util.List;

/**
 * It is a join operator node
 */
public class JoinPlan extends Plan {
    // TODO
    private List<Scheme> schemes;
    private List<String> columnNames;

    public JoinPlan(List<Plan> childrenPlanNodes, List<Scheme> schemes, List<String> columnNames) {
        super(childrenPlanNodes);
        this.pt = planType.JOIN;
        this.schemes = schemes;
        this.columnNames = columnNames;
    }

    public List<Scheme> getSchemes() {
        return schemes;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }
}
