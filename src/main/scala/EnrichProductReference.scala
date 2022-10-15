import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{col, when}
import com.typesafe.config.{Config, ConfigFactory}
import ajfunctions.read_schema
import java.time.LocalDate

object EnrichProductReference {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(name="EnrichProductReference").master(master= "local[*]").getOrCreate()
    val ajconfig: Config =  ConfigFactory.load("application.conf")
    val inputLocation = ajconfig.getString("paths.inputLocation")
    val OutputLocation = ajconfig.getString("paths.outputLocation")

    val dateToday = LocalDate.now()
    val yesterDate = dateToday.minusDays(1)
    //val currDayZoneSuffix = "_"+dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val currDayZoneSuffix = "_18072020"
    //val prevDayZoneSuffix = "_"+yesterDate.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val prevDayZoneSuffix = "_17072020"

    val lfs_from_file = ajconfig.getString("schema.landingFileSchema")
    val lfs = read_schema(lfs_from_file)

    //Reading Valid Data
    val validDataDF = spark.read
      .schema(lfs)
      .option("delimiter","|")
      .option("header",true)
      .csv(OutputLocation+"Valid\\ValidData"+currDayZoneSuffix)
    validDataDF.createOrReplaceTempView("validData")
    //validDataDF.show()

    val pfs_from_file = ajconfig.getString("schema.productFileSchema")
    val pfs = read_schema(pfs_from_file)

    //Reading Product Price Reference
    val productPriceReferenceDF = spark.read
      .schema(pfs)
      .option("delimiter","|")
      .option("header",true)
      .csv(inputLocation+"Products")
    productPriceReferenceDF.createOrReplaceTempView("productPriceReference")
    //productPriceReferenceDF.show()

    val productEnrichedDF = spark.sql("select a.Sale_ID,a.Product_ID,b.Product_Name,a.Quantity_Sold,a.Vendor_ID,a.Sale_Date,a.Quantity_Sold*b.Product_Price as Sale_Amount,a.Sale_Currency " +
      "from validData a inner join productPriceReference b "+
      "on a.Product_ID=b.Product_ID")
    //productEnrichedDF.show()

    productEnrichedDF.write
      .mode("overwrite")
      .option("delimiter","|")
      .option("header",true)
      .csv(OutputLocation+"Enriched\\SaleAmountEnrichment\\SaleAmountEnriched"+currDayZoneSuffix)



  }
}
