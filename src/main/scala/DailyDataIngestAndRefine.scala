import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{col, when}
import com.typesafe.config.{Config, ConfigFactory}
import ajfunctions.read_schema
import java.time.LocalDate
import org.apache.spark.sql.functions.col
import java.time.format.DateTimeFormatter

object DailyDataIngestAndRefine {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(name="DailyDataIngestAndRefine").master(master= "local[*]").getOrCreate()
    val sc= spark.sparkContext
    import spark.implicits._
    // Reading landing data from config file
    val ajconfig: Config =  ConfigFactory.load("application.conf")
    val inputLocation = ajconfig.getString("paths.inputLocation")
    val OutputLocation = ajconfig.getString("paths.outputLocation")
    val lfs_from_file = ajconfig.getString("schema.landingFileSchema")
    val hfs_from_file = ajconfig.getString("schema.holdFileSchema")
    val lfs = read_schema(lfs_from_file)
    val hfs = read_schema(hfs_from_file)
   /* val landingFileSchema = StructType(List(
      StructField("Sale_ID",StringType,true),
      StructField("Product_ID",StringType,true),
      StructField("Quantity_Sold",IntegerType,true),
      StructField("Vendor_ID",StringType,true),
      StructField("Sale_Date",TimestampType,true),
      StructField("Sale_Amount",DoubleType,true),
      StructField("Sale_Currency",StringType,true)
    ))*/
    //Handling Dates
    val dateToday = LocalDate.now()
    val yesterDate = dateToday.minusDays(1)
    //val currDayZoneSuffix = "_"+dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val currDayZoneSuffix = "_19072020"
    //val prevDayZoneSuffix = "_"+yesterDate.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val prevDayZoneSuffix = "_18072020"

    println(prevDayZoneSuffix)
    println(currDayZoneSuffix)

    val landingFileDF= spark.read.schema(lfs)
      .option("delimiter","|")
      .csv(inputLocation+"Sales_Landing\\SalesDump"+currDayZoneSuffix)
    landingFileDF.createOrReplaceTempView("landingFileDF")
    landingFileDF.show()

    //Checking if updates were received for any previously held data
    val previousHoldDF = spark.read.schema(hfs)
      .option("delimiter","|")
      .option("header",true)
      .csv(OutputLocation+"Hold\\HoldData"+prevDayZoneSuffix)
    previousHoldDF.createOrReplaceTempView("previousHoldDF")
    previousHoldDF.show()

    val refreshedLandingData = spark.sql("select a.Sale_ID,a.Product_ID,"+
      "case "+
      "when (a.Quantity_Sold is null) then b.Quantity_Sold "+
      "else a.Quantity_Sold "+
      "end as Quantity_Sold, "+
      "case "+
      "when (a.Vendor_ID is null) then b.Vendor_ID "+
      "else a.Vendor_ID "+
      "end as Vendor_ID, "+
      "a.Sale_Date,a.Sale_Amount,a.Sale_Currency "+
      "from landingFileDF a left outer join previousHoldDF b "+
      "on a.Sale_ID = b.Sale_ID")
    refreshedLandingData.createOrReplaceTempView("refreshedLandingData")
    refreshedLandingData.show()

    val validLandingData = refreshedLandingData.filter(col("Quantity_Sold").isNotNull && col("Vendor_ID").isNotNull )
    // validLandingData.show()
    validLandingData.createOrReplaceTempView("validLandingData")

    val releasedFromHold = spark.sql("select a.Sale_ID "+
      "from validLandingData a inner join previousHoldDF b "+
      "on a.Sale_ID = b.Sale_ID"
    )
    releasedFromHold.createOrReplaceTempView("releasedFromHold")


    val notReleasedFromHold = spark.sql("select * from previousHoldDF "+
      "where Sale_ID not in (select Sale_ID from releasedFromHold)")
    notReleasedFromHold.createOrReplaceTempView("notReleasedFromHold")
    //landingFileDF.show()

   val invalidLandingData = refreshedLandingData.filter(col("Quantity_Sold").isNull || col("Vendor_ID").isNull )
     .withColumn("Hold_Reason", when(col("Quantity_Sold").isNull,"Qty sold is missing")
     .otherwise(when(col("Vendor_ID").isNull, "Vendor_ID is missing")))
     .union(notReleasedFromHold)
    //invalidLandingData.show()
    validLandingData.write
      .mode("overwrite")
      .option("delimiter","|")
      .option("header",true)
      .csv(OutputLocation+"Valid\\ValidData"+currDayZoneSuffix)

    invalidLandingData.write
      .mode("overwrite")
      .option("delimiter","|")
      .option("header",true)
      .csv(OutputLocation+"Hold\\HoldData"+currDayZoneSuffix)
  }
}
