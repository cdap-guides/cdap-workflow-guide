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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.schedule.Schedules;

/**
 * Workflow application demonstrating fork and condition nodes in a Workflow.
 */
public class PurchaseWorkflowApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("PurchaseWorkflowApp");
    setDescription("Application describing the Workflow");

    addStream(new Stream("purchaseEvents"));

    addMapReduce(new PurchaseEventParser());
    addMapReduce(new PurchaseCounterByCustomer());
    addMapReduce(new PurchaseCounterByProduct());
    addWorkflow(new PurchaseWorkflow());

    scheduleWorkflow(Schedules.builder("HourlySchedule")
                       .setDescription("Schedule execution every 1 hour")
                       .createTimeSchedule("0 * * * *"),
                     "PurchaseWorkflow");

    addService(new PurchaseResultService());

    createDataset("purchaseRecords", KeyValueTable.class);
    createDataset("customerPurchases", KeyValueTable.class);
    createDataset("productPurchases", KeyValueTable.class);
  }
}
