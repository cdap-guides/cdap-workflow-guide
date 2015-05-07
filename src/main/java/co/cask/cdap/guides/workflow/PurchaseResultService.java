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
 * Service handling the requests for the purchase records for users and products.
 */
public class PurchaseResultService extends AbstractService {

  @Override
  protected void configure() {
    setName("PurchaseResultService");
    setDescription("Service to query for the purchases made by user and product.");
    addHandler(new PurchaseResultServiceHandler());
  }

  public class PurchaseResultServiceHandler extends AbstractHttpServiceHandler {

    @UseDataSet("productPurchases")
    private KeyValueTable productPurchases;

    @UseDataSet("userPurchases")
    private KeyValueTable userPurchases;

    @UseDataSet("purchaseRecords")
    private KeyValueTable purchaseRecords;

    @GET
    @Path("purchaserecords/{user-id}")
    public void getPurchaseRecord(HttpServiceRequest request, HttpServiceResponder responder,
                                  @PathParam("user-id") String userId) {
      byte[] value = purchaseRecords.read(userId);
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
    @Path("purchases/users/{user-id}")
    public void getPurchasesByUser(HttpServiceRequest request, HttpServiceResponder responder,
                                   @PathParam("user-id") String userId) {
      byte[] value = userPurchases.read(userId);
      if (value == null) {
        responder.sendStatus(404);
        return;
      }
      responder.sendString(String.valueOf(Bytes.toInt(value)));
    }
  }
}
