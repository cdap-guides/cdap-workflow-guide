package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Service handling the requests for the purchase records for customers and products.
 */
public class PurchaseResultService extends AbstractService {

  @Override
  protected void configure() {
    setName("PurchaseResultService");
    setDescription("Service to query for the purchases made by customer and product.");
    addHandler(new PurchaseResultServiceHandler());
  }

  public class PurchaseResultServiceHandler extends AbstractHttpServiceHandler {

    @UseDataSet("productPurchases")
    private KeyValueTable productPurchases;

    @UseDataSet("customerPurchases")
    private KeyValueTable customerPurchases;

    @UseDataSet("purchaseRecords")
    private KeyValueTable purchaseRecords;

    @GET
    @Path("purchaserecords/{customer-id}")
    public void getPurchaseRecord(HttpServiceRequest request, HttpServiceResponder responder,
                                  @PathParam("customer-id") String customerId) {
      byte[] value = purchaseRecords.read(customerId);
      if (value == null) {
        responder.sendStatus(404);
        return;
      }
      responder.sendString(Bytes.toString(value));
    }

    @GET
    @Path("purchases/products/{product-id}")
    public void getPurchaseByProduct(HttpServiceRequest request, HttpServiceResponder responder,
                                     @PathParam("product-id") String productId) {
      byte[] value = productPurchases.read(productId);
      if (value == null) {
        responder.sendStatus(404);
        return;
      }
      responder.sendString(String.valueOf(Bytes.toInt(value)));
    }

    @GET
    @Path("purchases/customers/{customer-id}")
    public void getPurchasesByCustomer(HttpServiceRequest request, HttpServiceResponder responder,
                                   @PathParam("customer-id") String customerId) {
      byte[] value = customerPurchases.read(customerId);
      if (value == null) {
        responder.sendStatus(404);
        return;
      }
      responder.sendString(String.valueOf(Bytes.toInt(value)));
    }
  }
}
