package co.cask.cdap.guides.workflow;

import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;

/**
 * This class represents a purchase made by a customer. It is a very simple class and only contains
 * the name of the customer, the name of the product, product quantity, price paid, and the purchase time.
 */
public class Purchase {

  public static final Type LIST_PURCHASE_TYPE = new TypeToken<List<Purchase>>() { }.getType();

  private final String customer, product;
  private final int quantity, price;
  private final long purchaseTime;

  public Purchase(String customer, String product, int quantity, int price, long purchaseTime) {
    this.customer = customer;
    this.product = product;
    this.quantity = quantity;
    this.price = price;
    this.purchaseTime = purchaseTime;
  }

  public String getCustomer() {
    return customer;
  }

  public String getProduct() {
    return product;
  }

  public long getPurchaseTime() {
    return purchaseTime;
  }

  public int getQuantity() {
    return quantity;
  }

  public int getPrice() {
    return price;
  }

  /**
   * Parse a sentence describing a purchase, of the form: <name> bought <n> <items> for $<price>
   */
  public static Purchase parse(String sentence) {
    try {
      String[] tokens =  sentence.split(" ");
      if (tokens.length != 6) {
        return null;
      }
      if (!"bought".equals(tokens[1]) || !"for".equals(tokens[4])) {
        return null;
      }
      String customer = tokens[0];
      String item = tokens[3];
      String price = tokens[5];
      if (!price.startsWith("$")) {
        return null;
      }
      int quantity = Integer.parseInt(tokens[2]);
      int amount = Integer.parseInt(tokens[5].substring(1));
      if (quantity <= 0 || amount <= 0) {
        return null;
      }
      if (quantity != 1 && item.length() > 1 && item.endsWith("s")) {
        item = item.substring(0, item.length() - 1);
      }
      return new Purchase(customer, item, quantity, amount, System.currentTimeMillis());

    } catch (NumberFormatException e) {
      return null;
    }
  }
}
