package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;

/**
 * Verifier that returns boolean value based on the number of records processed by the {@link PurchaseEventParser}.
 */
public class PurchaseEventVerifier implements Predicate<WorkflowContext> {

  @Override
  public boolean apply(WorkflowContext workflowContext) {
    if (workflowContext == null) {
      return false;
    }

    WorkflowToken token = workflowContext.getToken();
    if (token == null) {
      return false;
    }

    String taskCounterGroupName = "org.apache.hadoop.mapreduce.TaskCounter";
    String mapInputRecordsCounterName = "MAP_INPUT_RECORDS";

    Value mapInputRecords = token.get(taskCounterGroupName + "." + mapInputRecordsCounterName,
                                      WorkflowToken.Scope.SYSTEM);

    String mapOutputRecordsCounterName = "MAP_OUTPUT_RECORDS";
    Value mapOutputRecords = token.get(taskCounterGroupName + "." + mapOutputRecordsCounterName,
                                       WorkflowToken.Scope.SYSTEM);

    if (mapInputRecords == null || mapOutputRecords == null) {
      return false;
    }

    // Return true if at least 80% of the records were successfully parsed and emitted
    // by previous map job
    return (mapOutputRecords.getAsLong() >= (mapInputRecords.getAsLong() * 80 / 100));
  }
}
