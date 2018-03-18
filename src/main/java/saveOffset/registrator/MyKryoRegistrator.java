package saveOffset.registrator;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import saveOffset.key.BizMinuteUrlIpKey;
import saveOffset.key.BizMinuteUrlKey;
import saveOffset.model.NginxLogModel;

public class MyKryoRegistrator implements KryoRegistrator
{
  @Override
  public void registerClasses(Kryo kryo)
  {
    kryo.register(NginxLogModel.class);
    kryo.register(BizMinuteUrlIpKey.class);
    kryo.register(BizMinuteUrlKey.class);
  }
}
