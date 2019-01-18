package cn.tarena.kryo

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import cn.tarena.Info
import cn.tarena.Tongji
import cn.tarena.Tongji2

class MyKryoRegister extends KryoRegistrator {
  
   //序列化   
  def registerClasses(kryo: Kryo): Unit = {
    //--将指定的类的序列化替换成kryo
    //--用这种方式，可以将指定的多个类的序列化替换
    kryo.register(classOf[Info])
    kryo.register(classOf[Tongji])
    kryo.register(classOf[Tongji2])
  }
}