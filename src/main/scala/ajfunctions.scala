import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType}
object ajfunctions {
def read_schema(schema_arg : String): StructType = {
  var sch : StructType = new StructType()
  val split_values = schema_arg.split(",").toList
  val d_types = Map(
    "StringType" -> StringType,
    "IntegerType" -> IntegerType,
    "TimestampType" -> TimestampType,
    "DoubleType" -> DoubleType,
    "FloatType" -> FloatType
  )
  for(i<-split_values){
    val columnVal = i.split(" ").toList
    sch = sch.add(columnVal(0),d_types(columnVal(1)),nullable = true)
  }
  return sch
  }
}
