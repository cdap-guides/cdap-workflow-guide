package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import com.google.gson.Gson;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * MapReduce program to compute total purchases by customer.
 */
public class PurchaseCounterByCustomer extends AbstractMapReduce {

  @Override
  public void configure() {
    setDescription("Purchases Counter by Customer");
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();
    job.setMapperClass(PerCustomerMapper.class);
    job.setReducerClass(PerCustomerReducer.class);

    context.addInput(Input.ofDataset("purchaseRecords"));
    context.addOutput(Output.ofDataset("customerPurchases"));
  }

  /**
   * Mapper class to emit customer and corresponding purchase count information.
   */
  public static class PerCustomerMapper extends Mapper<byte[], byte[], Text, LongWritable> {

    @Override
    public void map(byte[] key, byte[] value, Context context)
      throws IOException, InterruptedException {
      String purchaseJson = Bytes.toString(value);
      List<Purchase> customerPurchases = new Gson().fromJson(purchaseJson, Purchase.LIST_PURCHASE_TYPE);
      long purchaseValue = 0;
      for (Purchase p : customerPurchases) {
        purchaseValue += p.getPrice();
      }
      context.write(new Text(Bytes.toString(key)), new LongWritable(purchaseValue));
    }
  }

  /**
   * Reducer class to aggregate all purchases per customer.
   */
  public static class PerCustomerReducer extends Reducer<Text, LongWritable, byte[], byte[]>
    implements ProgramLifecycle<MapReduceContext> {

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      // no-op
    }

    @Override
    public void reduce(Text customer, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
      int totalPurchase = 0;
      for (LongWritable val : values) {
        totalPurchase += val.get();
      }
      context.write(Bytes.toBytes(customer.toString()), Bytes.toBytes(totalPurchase));
    }

    @Override
    public void destroy() {
      // no-op
    }
  }
}
