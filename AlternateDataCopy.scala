import java.security.PrivilegedExceptionAction
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
System.setProperty("java.security.krb5.conf", "krb5.conf");
System.setProperty("java.security.krb5.realm", "BLUEDART.COM");
System.setProperty("java.security.krb5.kdc", "SINHQCDC11.bluedart.com");

val configuration = new Configuration()
configuration.set("hadoop.security.authentication", "kerberos");
configuration.set("hbase.rest.authentication.kerberos.keytab", "/home/datalake/ControlTowerProd/CommonFiles/keytab/hbase.keytab");
configuration.set("hbase.master.keytab.file", "/home/datalake/ControlTowerProd/CommonFiles/keytab/hbase.keytab");
configuration.set("hbase.regionserver.keytab.file", "/home/datalake/ControlTowerProd/CommonFiles/keytab/hbase.keytab");
configuration.set("hbase.cluster.distributed", "true");
configuration.set("hbase.rpc.protection", "authentication");
configuration.set("hbase.client.retries.number", "5");
configuration.set("hbase.regionserver.kerberos.principal", "hbase/sinhqcbiprden002.bluedart.com@BLUEDART.COM");
configuration.set("hbase.master.kerberos.principal", "hbase/sinhqcbiprden002.bluedart.com@BLUEDART.COM");
UserGroupInformation.setConfiguration(configuration);
val dateFmt = "dd-MMM-YYYY HH:mm:ss"

val loginUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hbase/sinhqcbiprden002.bluedart.com@BLUEDART.COM", "hbase.keytab")
println("Login user created :>> " + loginUser)
loginUser.doAs(new PrivilegedExceptionAction[Unit] {
override def run: Unit = {
//creating a configuration object

//val tableName = "CT_INCREMENTAL_LOAD"
/* var IL_tableName_ADT : String = null
var PL_tableName_ADT : String = null
var IL_tableName_AWBNO : String = null
var PL_tableName_AWBNO : String = null*/
val sdf = new SimpleDateFormat(dateFmt)
val config = HBaseConfiguration.create()
//      config.clear()
config.set("hbase.zookeeper.quorum", "172.18.114.95")
config.set("hbase.zookeeper.property.clientPort", "2181")
config.set("hbase.regionserver.compaction.enabled","false")
//config.set(TableInputFormat.INPUT_TABLE, IL_tableName)

println(">>> config >>> " + config)

//Creating HBaseAdmin object
val admin = new HBaseAdmin(config)

// val tableName_ADT = "CT_LOAD_AWBID"
//val IL_tableName_AWBNO = "CT_IL_LOAD_LOAD"
//val PL_tableName_CPS = "CT_THREAD_TOKENNO"
val PL_tableName_AWBNO = "CTOWER.CT_PL_CAWBNO"
//val PL_tableName_AWBNO = "TEST_PL_AWBNO"
//val IL_tableName_COAWBNO = "CT_THREAD_IL_LOAD_COAWBNO"
//val PL_tableName_COAWBNO = "CT_THREAD_PL_LOAD_COAWBNO"
config.set(TableInputFormat.INPUT_TABLE, PL_tableName_AWBNO)



val hBaseRDD = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

import sqlContext.implicits._

val df_hbasetab = hBaseRDD.map(x => {
(
Bytes.toString(x._2.getRow),
Bytes.toString(x._2.getValue(Bytes.toBytes("ALK"), Bytes.toBytes("CT_COL"))),
Bytes.toString(x._2.getValue(Bytes.toBytes("SRT"), Bytes.toBytes("CT_COL"))),
Bytes.toString(x._2.getValue(Bytes.toBytes("SRJ"), Bytes.toBytes("CT_COL")))

)
}).toDF().withColumnRenamed("_1", "PK").withColumnRenamed("_2", "ALK").withColumnRenamed("_3", "SRT").withColumnRenamed("_4", "SRJ")

df_hbasetab.registerTempTable("hbasetab")

val alk_df = sqlContext.sql("""SELECT PK,ALK as CT_COL from hbasetab where SPLIT(ALK,'~')[0]!='NA!@#'""")
//println("ALK count:"+alk_df.count)
val srt_df = sqlContext.sql("""SELECT PK,SRT as CT_COL from hbasetab where SPLIT(SRT,'~')[0]!='NA!@#'""")
//println("SRT count:"+srt_df.count)
val srj_df = sqlContext.sql("""SELECT PK,SRJ as CT_COL from hbasetab where SPLIT(SRJ,'~')[0]!='NA!@#'""")
//println("SRJ count:"+srj_df.count)

println("Current timestamp:"+sdf.format(new Date()))
val PL_tableName_COAWBNO = "CT_THREAD_PL_LOAD_COAWBNO"

val pl_map2 = Map("SRJ" -> srj_df, "SRT" -> srt_df,"ALK" -> alk_df)

pl_map2.foreach { a: (String, DataFrame) =>
val key = a._1
val value = a._2

println(s"key: $key ")

def PL_catalog2 = s"""{
|"table":{"namespace":"default", "name":"${PL_tableName_COAWBNO}"},
|"rowkey":"PK",
|"columns":{
|"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
|"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
|}
|}""".stripMargin

println("catalog created :::: " + PL_catalog2)

println(sdf.format(new Date()))

value.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_catalog2, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

println("catalog " + key + " written to hbase table:::: " + sdf.format(new Date()))

}

}
})