package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This is a custom action in the Workflow that could be used to send email notifications.
 */
public class ProblemLogger extends AbstractWorkflowAction {

  private static final Logger LOG = LoggerFactory.getLogger(ProblemLogger.class);
  private static final String TASK_COUNTER_GROUP_NAME = "org.apache.hadoop.mapreduce.TaskCounter";

  private WorkflowContext context;

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    this.context = context;
  }

  @Override
  public void run() {
    long mapInputRecordNumber = getCounterValue(TASK_COUNTER_GROUP_NAME, "MAP_INPUT_RECORDS");
    long mapOutputRecordNumber = getCounterValue(TASK_COUNTER_GROUP_NAME, "MAP_OUTPUT_RECORDS");

    LOG.info("Found '{}' malformed events out of total '{}' received events. Send email notification about the " +
               "higher number of malformed events.", mapInputRecordNumber - mapOutputRecordNumber,
             mapInputRecordNumber);
  }

  /**
   * Return the value of the hadoop counter.
   * @param groupName The name of the counter group
   * @param counterName The name of the counter
   * @return The value of the counter. If no counter found return 0.
   */
  private long getCounterValue(String groupName, String counterName) {

    WorkflowToken token = context.getToken();

    Map<String, Map<String, Long>> hadoopCounters = token.getMapReduceCounters();
    if (hadoopCounters == null) {
      return 0;
    }

    Map<String, Long> taskCounter = hadoopCounters.get(groupName);

    if (taskCounter.containsKey(counterName)) {
      return taskCounter.get(counterName);
    }
    return 0;
  }
}
