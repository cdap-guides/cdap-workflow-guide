package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

/**
 * Workflow application explaining fork and condition nodes in the Workflow.
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

    addService(new PurchaseResultService());

    createDataset("purchaseRecords", KeyValueTable.class);
    createDataset("userPurchases", KeyValueTable.class);
    createDataset("productPurchases", KeyValueTable.class);
  }
}