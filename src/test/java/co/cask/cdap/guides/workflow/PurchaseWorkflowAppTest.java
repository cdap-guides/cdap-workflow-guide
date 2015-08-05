package co.cask.cdap.guides.workflow;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class PurchaseWorkflowAppTest extends TestBase {

  @Test
  public void test() throws Exception {
    ApplicationManager appManager = deployApplication(PurchaseWorkflowApp.class);

    StreamManager streamManager = getStreamManager("purchaseEvents");
    streamManager.send("bad formatted event data");
    streamManager.send("another bad formatted event data");
    streamManager.send("bob bought 3 apples for $15");

    WorkflowManager workflowManager = appManager.getWorkflowManager("PurchaseWorkflow").start();
    workflowManager.waitForFinish(3, TimeUnit.MINUTES);

    // Start the service
    ServiceManager serviceManager = appManager.getServiceManager(PurchaseResultService.NAME).start();

    // Wait for service startup
    serviceManager.waitForStatus(true);

    URL url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "purchaserecords/bob");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

    url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "purchases/customers/bob");
    conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, conn.getResponseCode());

    url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "purchases/products/apple");
    conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, conn.getResponseCode());

    streamManager.send("joe bought 1 apple for $7");
    streamManager.send("joe bought 10 pineapples for $85");
    streamManager.send("cat bought 3 bottles for $12");
    streamManager.send("bob bought 2 pops for $14");

    workflowManager =  appManager.getWorkflowManager("PurchaseWorkflow").start();
    workflowManager.waitForFinish(3, TimeUnit.MINUTES);

    url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "purchaserecords/bob");
    conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

    url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "purchases/customers/bob");
    conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

    String purchaseValue;
    try {
      purchaseValue = new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
    Assert.assertEquals("29", purchaseValue);

    url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "purchases/products/apple");
    conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

    String totalSellForApples;
    try {
      totalSellForApples = new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
    Assert.assertEquals("22", totalSellForApples);
  }
}
