package txDB.execution.plans;

import txDB.storage.table.Scheme;

import java.util.List;

public class PredEvalPlan extends Plan {
    // From BusTub in CMU
    public enum comparisonType { Equal, NotEqual, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual }
    public enum logicType { AND, OR, NOT }
    private Scheme scheme;
    private List<String> columnNames;
    private List<comparisonType> comparisonTypes;
    private List<logicType> logicTypes;
    private List<Object> values;

    public PredEvalPlan(List<Plan> childrenPlanNodes, Scheme scheme, List<String> columnNames, List<comparisonType> comparisonTypes, List<logicType> logicTypes, List<Object> values) {
        super(childrenPlanNodes);
        this.pt = planType.PREDEVAL;
        this.scheme = scheme;
        this.columnNames = columnNames;
        this.comparisonTypes = comparisonTypes;
        this.logicTypes = logicTypes;
        this.values = values;
    }

    public Scheme getScheme() {
        return scheme;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<comparisonType> getComparisonTypes() {
        return comparisonTypes;
    }

    public List<logicType> getLogicTypes() {
        return logicTypes;
    }

    public List<Object> getValues() {
        return values;
    }
}
