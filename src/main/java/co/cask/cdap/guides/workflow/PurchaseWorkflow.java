package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.workflow.AbstractWorkflow;

/**
 * Workflow for processing purchase events and computing total purchases made by particular user and total purchases
 * done for the particular product.
 */
public class PurchaseWorkflow extends AbstractWorkflow {
  @Override
  protected void configure() {
    setName("PurchaseWorkflow");
    setDescription("Workflow to parse the purchase events and count the purchases per user and per product");

    addMapReduce("PurchaseEventParser");

    condition(new PurchaseEventVerifier())
      .fork()
        .addMapReduce("PurchaseCounterByUser")
      .also()
        .addMapReduce("PurchaseCounterByProduct")
      .join()
    .otherwise()
      .addAction(new EmailNotifier())
    .end();
  }
}
