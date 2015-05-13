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
    addMapReduce(new PurchaseCounterByUser());
    addMapReduce(new PurchaseCounterByProduct());
    addWorkflow(new PurchaseWorkflow());

    scheduleWorkflow(Schedules.createTimeSchedule("HourlySchedule", "Schedule execution every 1 hour", "0 * * * *"),
                     "PurchaseWorkflow");

    addService(new PurchaseResultService());

    createDataset("purchaseRecords", KeyValueTable.class);
    createDataset("userPurchases", KeyValueTable.class);
    createDataset("productPurchases", KeyValueTable.class);
  }
}
