package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.workflow.AbstractWorkflow;

/**
 * Workflow for processing purchase events and computing total purchases made by
 * a each customer and total purchases for a each product.
 */
public class PurchaseWorkflow extends AbstractWorkflow {
  @Override
  protected void configure() {
    setName("PurchaseWorkflow");
    setDescription("Workflow to parse the purchase events and count the purchases per customer and per product");

    addMapReduce("PurchaseEventParser");

    condition(new PurchaseEventVerifier())
      .fork()
        .addMapReduce("PurchaseCounterByCustomer")
      .also()
        .addMapReduce("PurchaseCounterByProduct")
      .join()
    .otherwise()
      .addAction(new ProblemLogger())
    .end();
  }
}
