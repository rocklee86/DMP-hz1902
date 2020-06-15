package cn.qphone.dmp.etl

import java.util.Properties

import cn.qphone.dmp.traits.LoggerTrait
import cn.qphone.dmp.utils.{CommonUtils, SparkUtils}
import org.apache.spark.sql.SaveMode

object Data2Mysql extends LoggerTrait{
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

        //2. 获取数据
        val df = spark.read.parquet(input)

        //3. 注册视图
        df.createTempView("log")

        //4. sql
        val ret = spark.sql(
            """
              |select
              |count(*) ct,
              |provincename,
              |cityname
              |from
              |`log`
              |group by provincename, cityname
              |""".stripMargin)

        //5. 输出
//        ret.coalesce(1).write.json(output)
        val jdbc = CommonUtils.toMap("db.properties")
        properties.setProperty("user", jdbc("mysql.username"))
        properties.setProperty("password", jdbc("mysql.password"))
        ret.write.mode(SaveMode.Append).jdbc(jdbc("mysql.url"), "dmp", properties)

        //6. 释放
//        SparkUtils.stop(spark)
    }
}
