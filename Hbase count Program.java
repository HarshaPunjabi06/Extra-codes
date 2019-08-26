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

val df_hbasepl = hBaseRDD.map(x => {
(
Bytes.toString(x._2.getRow),
Bytes.toString(x._2.getValue(Bytes.toBytes("PDJ"), Bytes.toBytes("CT_COL"))),
Bytes.toString(x._2.getValue(Bytes.toBytes("ABM"), Bytes.toBytes("CT_COL"))),
Bytes.toString(x._2.getValue(Bytes.toBytes("SLT"), Bytes.toBytes("CT_COL"))),
Bytes.toString(x._2.getValue(Bytes.toBytes("DLS"), Bytes.toBytes("CT_COL"))),
Bytes.toString(x._2.getValue(Bytes.toBytes("DTS"), Bytes.toBytes("CT_COL")))
)
}).toDF().withColumnRenamed("_1", "PK").withColumnRenamed("_2", "PDJ").withColumnRenamed("_3", "ABM").withColumnRenamed("_4", "SLT").withColumnRenamed("_5", "DLS").withColumnRenamed("_6", "DTS")

df_hbasepl.filter('PK.isin("42436173334","42436194640","42436398550","42436438951")).show(false)

df_hbasepl.registerTempTable("plhbase")

val closure_codes = "'123','016','026','132','074','000','135','141','021','025','099','105','070','090','098','188','178','284'"

val dls_updateDF = sqlContext.sql(s"""Select PK, NVL(CASE WHEN (ABM = "NA!@#" or ABM is null) AND (SLT = "NA!@#" or SLT is null) AND (PDJ = "NA!@#" or PDJ IS NULL) THEN 'I' ELSE CASE WHEN (ABM = "NA!@#" or ABM is null) AND (SLT = "NA!@#" or SLT is null) AND (PDJ != "NA!@#" or PDJ is NOT NULL) THEN CASE WHEN (instr(PDJ,'S015') <= 0 AND instr(PDJ,'S018') <= 0 ) THEN 'I' ELSE CASE WHEN (instr(PDJ,'S015') > 0 OR instr(PDJ,'S018') > 0 ) AND (DATEDIFF(CURRENT_DATE(), CAST(substr(PDJ, instr(PDJ, '~S015~') - 19, 16) AS  DATE)) > 45 OR DATEDIFF(CURRENT_DATE(), CAST(substr(PDJ, instr(PDJ, '~S018~') - 19, 16) AS DATE)) > 45) THEN 'I' ELSE 'UD' END END ELSE
CASE WHEN (ABM != "NA!@#" or ABM is NOT NULL OR SLT != "NA!@#" or SLT is NOT NULL) AND (PDJ = "NA!@#" or PDJ IS NULL) THEN 'NA!@#' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T123~') > 0 THEN substr(PDJ,instr(PDJ, '~T123~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T016~') > 0 THEN substr(PDJ,instr(PDJ, '~T016~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T026~') > 0 THEN substr(PDJ,instr(PDJ, '~T026~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T132~') > 0 THEN substr(PDJ,instr(PDJ, '~T132~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T074~') > 0 THEN substr(PDJ,instr(PDJ, '~T074~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T000~') > 0 THEN substr(PDJ,instr(PDJ, '~T000~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T135~') > 0 THEN substr(PDJ,instr(PDJ, '~T135~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T141~') > 0 THEN substr(PDJ,instr(PDJ, '~T141~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T021~') > 0 THEN substr(PDJ,instr(PDJ, '~T021~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T025~') > 0 THEN substr(PDJ,instr(PDJ, '~T025~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T099~') > 0 THEN substr(PDJ,instr(PDJ, '~T099~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T105~') > 0 THEN substr(PDJ,instr(PDJ, '~T105~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T070~') > 0 THEN substr(PDJ,instr(PDJ, '~T070~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T090~') > 0 THEN substr(PDJ,instr(PDJ, '~T090~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T098~') > 0 THEN substr(PDJ,instr(PDJ, '~T098~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T188~') > 0 THEN substr(PDJ,instr(PDJ, '~T188~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T178~') > 0 THEN substr(PDJ,instr(PDJ, '~T178~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T284~') > 0 THEN substr(PDJ,instr(PDJ, '~T284~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T123~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T016~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T026~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T132~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T074~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T000~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T135~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T141~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T021~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T025~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T099~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T105~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T070~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T090~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T098~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T188~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T178~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T284~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
DLS END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END ,'NA!@#') as CT_COL from plhbase""")


dls_updateDF.filter('PK.isin("42436173334","42436194640","42436398550","42436438951")).show(false)
dls_updateDF.groupBy('CT_COL).count.show(false)

val dts_updateDF = sqlContext.sql(s"""Select PK, NVL(CASE WHEN (ABM = "NA!@#" or ABM is null) AND (SLT = "NA!@#" or SLT is null) AND (PDJ = "NA!@#" or PDJ IS NULL) THEN from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') ELSE
CASE WHEN (ABM = "NA!@#" or ABM is null) AND (SLT = "NA!@#" or SLT is null) AND (PDJ != "NA!@#" or PDJ is NOT NULL) THEN
CASE WHEN (instr(PDJ,'S015') <= 0 AND instr(PDJ,'S018') <=0 ) THEN from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') ELSE
CASE WHEN (instr(PDJ,'S015') > 0 OR instr(PDJ,'S018') > 0 ) AND (DATEDIFF(CURRENT_DATE(), CAST(substr(PDJ, instr(PDJ, '~S015~') - 19, 10) AS  DATE)) > 45 OR DATEDIFF(CURRENT_DATE(), CAST(substr(PDJ, instr(PDJ, '~S018~') - 19, 10) AS DATE)) > 45) THEN from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') ELSE
CASE WHEN locate("|",reverse(PDJ)) >0 THEN substr(PDJ,length(PDJ) - locate("|",reverse(PDJ))+2,16) ELSE  substr(PDJ,0,16) END END END ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T123~') > 0 THEN substr(PDJ,instr(PDJ, '~T123~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T016~') > 0 THEN substr(PDJ,instr(PDJ, '~T016~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T026~') > 0 THEN substr(PDJ,instr(PDJ, '~T026~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T132~') > 0 THEN substr(PDJ,instr(PDJ, '~T132~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T074~') > 0 THEN substr(PDJ,instr(PDJ, '~T074~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T000~') > 0 THEN substr(PDJ,instr(PDJ, '~T000~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T135~') > 0 THEN substr(PDJ,instr(PDJ, '~T135~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T141~') > 0 THEN substr(PDJ,instr(PDJ, '~T141~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T021~') > 0 THEN substr(PDJ,instr(PDJ, '~T021~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T025~') > 0 THEN substr(PDJ,instr(PDJ, '~T025~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T099~') > 0 THEN substr(PDJ,instr(PDJ, '~T099~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T105~') > 0 THEN substr(PDJ,instr(PDJ, '~T105~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T070~') > 0 THEN substr(PDJ,instr(PDJ, '~T070~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T090~') > 0 THEN substr(PDJ,instr(PDJ, '~T090~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T098~') > 0 THEN substr(PDJ,instr(PDJ, '~T098~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T188~') > 0 THEN substr(PDJ,instr(PDJ, '~T188~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T178~') > 0 THEN substr(PDJ,instr(PDJ, '~T178~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T284~') > 0 THEN substr(PDJ,instr(PDJ, '~T284~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND (PDJ != "NA!@#" or PDJ is NOT NULL) THEN CASE WHEN locate("|",reverse(PDJ)) >0 THEN substr(PDJ,length(PDJ) - locate("|",reverse(PDJ))+2,16) ELSE substr(PDJ,0,16) END  ELSE
DTS END END END END END END END END END END END END END END END END END END END END END ,'NA!@#') as CT_COL from plhbase""")

dts_updateDF.filter('PK.isin("42436173334","42436194640","42436398550","42436438951")).show(false)


}
})