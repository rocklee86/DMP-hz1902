package cn.qphone.dmp.app

import java.util.Properties

import cn.qphone.dmp.etl.Log2Parquet
import cn.qphone.dmp.traits.LoggerTrait
import cn.qphone.dmp.utils.SparkUtils

/**
 * 低于维度指标
 */
object LocationRpt extends LoggerTrait{
    def main(args: Array[String]): Unit = {
        //0.控制读取参数
        if (args == null || args.length != 2) {
            println("Usage : <input> <output>")
            System.exit(-1)
        }
        val Array(input, output) = args

        //1. 获取入口并配置序列化方式以及压缩方式
        //1.1 读取配置文件
        val properties = new Properties()
        properties.load(Log2Parquet.getClass.getClassLoader.getResourceAsStream("spark.properties"))
        //1.2 获取sparksession以及设置配置文件
        val spark = SparkUtils.getLocalSparkSession("Data2Mysql")
        spark.sqlContext.setConf(properties) // 这个压缩格式默认就是snappy，但是在spark1.6之前不是

        //2. 获取数据源
        val df = spark.read.parquet(input)

        //3. 创建虚拟视图
        df.createOrReplaceTempView("log")

        //4. 业务
        val result = spark.sql(
            """
              |select provincename,cityname,
              |sum(case when requestmode =1 and processnode >= 1 then 1 else 0 end) ysrequest,
              |sum(case when requestmode =1 and processnode >= 2 then 1 else 0 end) yxrequest,
              |sum(case when requestmode =1 and processnode = 3 then 1 else 0 end) adrequest,
              |sum(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end) cybid,
              |sum(case when iseffective =1 and isbilling =1 and iswin =1 and adorderid !=0 then 1 else 0 end) cyAccbid,
              |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end) shows,
              |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end) clicks,
              |sum(case when iseffective =1 and isbilling =1 and iswin =1 then winprice/1000 else 0.0 end) pricost,
              |sum(case when iseffective =1 and isbilling =1 and iswin =1 then adpayment/1000 else 0.0 end) adpay
              |from log group by provincename,cityname
              |""".stripMargin)

        //5. 数据存储
        result.write.partitionBy("provincename", "cityname").save(output)

        //6. 释放
        SparkUtils.stop(spark)
    }
}
