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
