/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.Value;
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

    Value value = token.get(groupName + "." + counterName);
    if (value == null) {
      return 0;
    }

    return value.getAsLong();
  }
}
