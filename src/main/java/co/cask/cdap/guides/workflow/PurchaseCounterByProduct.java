package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.common.Bytes;
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
import java.util.concurrent.TimeUnit;

/**
 * MapReduce program to compute total purchases by product.
 */
public class PurchaseCounterByProduct extends AbstractMapReduce {

  @Override
  public void configure() {
    setDescription("Purchases Counter by Product");
    setInputDataset("purchaseRecords");
    setOutputDataset("productPurchases");
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();
    job.setMapperClass(PerProductMapper.class);
    job.setReducerClass(PerProductReducer.class);
    TimeUnit.SECONDS.sleep(5);
  }

  /**
   * Mapper class to emit product and corresponding purchase information.
   */
  public static class PerProductMapper extends Mapper<byte[], byte[], Text, LongWritable> {

    @Override
    public void map(byte[] key, byte[] value, Context context)
      throws IOException, InterruptedException {
      String purchaseJson = Bytes.toString(value);
      List<Purchase> customerPurchases = new Gson().fromJson(purchaseJson, Purchase.LIST_PURCHASE_TYPE);
      for (Purchase p : customerPurchases) {
        context.write(new Text(p.getProduct()), new LongWritable(p.getPrice()));
      }
    }
  }

  /**
   * Reducer class to aggregate all purchases per product.
   */
  public static class PerProductReducer extends Reducer<Text, LongWritable, byte[], byte[]>
    implements ProgramLifecycle<MapReduceContext> {

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      // no-op
    }

    @Override
    public void reduce(Text product, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
      int totalPurchase = 0;
      for (LongWritable val : values) {
        totalPurchase += val.get();
      }
      context.write(Bytes.toBytes(product.toString()), Bytes.toBytes(totalPurchase));
    }

    @Override
    public void destroy() {
      // no-op
    }
  }
}
