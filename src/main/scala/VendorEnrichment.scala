import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{col, when}
import com.typesafe.config.{Config, ConfigFactory}
import ajfunctions.read_schema
import java.time.LocalDate
object VendorEnrichment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("VendorEnrichment").master("local[*]").getOrCreate()
    val ajconfig: Config =  ConfigFactory.load("application.conf")
    val inputLocation = ajconfig.getString("paths.inputLocation")
    val OutputLocation = ajconfig.getString("paths.outputLocation")

    val dateToday = LocalDate.now()
    val yesterDate = dateToday.minusDays(1)
    //val currDayZoneSuffix = "_"+dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val currDayZoneSuffix = "_19072020"
    //val prevDayZoneSuffix = "_"+yesterDate.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val prevDayZoneSuffix = "_18072020"

    val pefs_from_file = ajconfig.getString("schema.productEnrichedFileSchema")
    val pefs = read_schema(pefs_from_file)

    val vrfs_from_file = ajconfig.getString("schema.vendorReferenceFileSchema")
    val vrfs = read_schema(vrfs_from_file)

    val urrs_from_file = ajconfig.getString("schema.usdRatesReferenceSchema")
    val urrs = read_schema(urrs_from_file)

    //Reading The Required Zones
     val productEnrichedDF = spark.read
       .schema(pefs)
       .option("header",true)
       .option("delimiter","|")
       .csv(OutputLocation+"Enriched\\SaleAmountEnrichment\\SaleAmountEnriched"+currDayZoneSuffix)
    productEnrichedDF.createOrReplaceTempView("productEnriched")

    val vendorReferenceDF = spark.read
      .schema(vrfs)
      .option("delimiter","|")
      .csv(inputLocation+"Vendors")
    vendorReferenceDF.createOrReplaceTempView("vendorReference")

    val usdRatesReferenceDF = spark.read
      .schema(urrs)
      .option("delimiter","|")
      .csv(inputLocation+"USD_Rates")
    usdRatesReferenceDF.createOrReplaceTempView("usdRatesReference")
    //usdRatesReferenceDF.show()

    val vendorEnrichedDF = spark.sql("select a.*,b.Vendor_Name " +
      "from productEnriched a inner join vendorReference b " +
      "on a.Vendor_ID=b.Vendor_ID")
    vendorEnrichedDF.createOrReplaceTempView("vendorEnriched ")
    //vendorEnrichedDF.show()

    val usdEnrichedDF = spark.sql("select *,round((a.Sale_Amount/b.Exchange_Rate),2) as Amount_USD " +
      "from vendorEnriched a join usdRatesReference b " +
      "on a.Sale_Currency = b.Currency_Code")
    usdEnrichedDF.createOrReplaceTempView("usdEnriched")
    //usdEnrichedDF.show()

   /*usdEnrichedDF.write
      .mode("overwrite")
      .option("delimiter","|")
      .option("header",true)
      .csv(OutputLocation+"Enriched\\Vendor_USD_Enriched\\Vendor_USD_Enriched"+ currDayZoneSuffix)
*/
    val reportDF = spark.sql("select Sale_ID,Product_ID,Product_Name,Quantity_Sold,Vendor_ID,Sale_Date,Sale_Amount,Sale_Currency, " +
      "Vendor_Name,Amount_USD " +
      "from usdEnriched")
    //reportDF.show()

    //Code For MySQL Connectivity
    /*reportDF.write.format("jdbc")
      .options(Map(
        "url"->"jdbc:mysql://localhost:3306/ajmartdb",
        "driver"->"com.mysql.jdbc.Driver",
        "dbtable"->"finalsales",
        "user"->"root",
        "password"->"Akshay@1999"
      )).mode("append")
      .save()*/
  }
}
