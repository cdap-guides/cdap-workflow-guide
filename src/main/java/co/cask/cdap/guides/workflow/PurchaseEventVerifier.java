package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;

/**
 * Verifier that returns boolean value based on the number of records processed by the {@link PurchaseEventParser}.
 */
public class PurchaseEventVerifier implements Predicate<WorkflowContext> {

  private static final String TASK_COUNTER_GROUP_NAME = "org.apache.hadoop.mapreduce.TaskCounter";
  private static final String MAP_INPUT_RECORDS_COUNTER_NAME = "MAP_INPUT_RECORDS";
  private static final String MAP_OUTPUT_RECORDS_COUNTER_NAME = "MAP_OUTPUT_RECORDS";

  @Override
  public boolean apply(WorkflowContext workflowContext) {
    if (workflowContext == null) {
      return false;
    }

    WorkflowToken token = workflowContext.getToken();
    if (token == null) {
      return false;
    }

    Value mapInputRecords = token.get(TASK_COUNTER_GROUP_NAME + "." + MAP_INPUT_RECORDS_COUNTER_NAME,
                                      WorkflowToken.Scope.SYSTEM);
    Value mapOutputRecords = token.get(TASK_COUNTER_GROUP_NAME + "." + MAP_OUTPUT_RECORDS_COUNTER_NAME,
                                       WorkflowToken.Scope.SYSTEM);
    if (mapInputRecords != null && mapOutputRecords != null) {
      // Return true if at least 80% of the records were successfully parsed and emitted
      // by previous map job
      return (mapOutputRecords.getAsLong() >= (mapInputRecords.getAsLong() * 80/100));
    }
    return false;
  }
}
