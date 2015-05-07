package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;

import java.util.Map;

/**
 * Verifier that returns boolean value based on the number of records processed by the {@link PurchaseEventParser}
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

    Map<String, Map<String, Long>> hadoopCounters = token.getMapReduceCounters();
    if (hadoopCounters == null) {
      return false;
    }

    Map<String, Long> taskCounter = hadoopCounters.get("org.apache.hadoop.mapreduce.TaskCounter");

    long mapInputRecordNumber = taskCounter.get("MAP_INPUT_RECORDS");
    long mapOutputRecordNumber = taskCounter.get("MAP_OUTPUT_RECORDS");

    // Return true if at least 80% of the records were processed by previous map job
    return (mapOutputRecordNumber >= (mapInputRecordNumber * 80/100));
  }
}
