import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection.universe._
/**
 * Convert schema to case class and case class to schema
 *
 * Created by swadhin on 27/01/17.
 */
object SchemaConverter {
  type TypeConverter = (DataType) => String

  def schemaToCaseClass(schema: StructType, className: String)(implicit tc: TypeConverter): String = {
    def genField(s: StructField): String = {
      val f = tc(s.dataType)
      s match {
        case x if x.nullable => s"  ${s.name}:Option[$f] = None"
        case _               => s"  ${s.name}:$f"
      }
    }

    val fieldsStr = schema.map(genField).mkString(",\n  ")
    s"""
       |case class $className (
       |  $fieldsStr
       |)
  """.stripMargin
  }

  def caseClassToSchema[T: TypeTag] = {
    ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
  }

  implicit val defaultTypeConverter: TypeConverter = {
    case _: ByteType      => "Byte"
    case _: ShortType     => "Short"
    case _: IntegerType   => "Int"
    case _: LongType      => "Long"
    case _: FloatType     => "Float"
    case _: DoubleType    => "Double"
    case _: DecimalType   => "BigDecimal"
    case _: StringType    => "String"
    case _: BinaryType    => "Array[Byte]"
    case _: BooleanType   => "Boolean"
    case _: TimestampType => "java.sql.Timestamp"
    case _: DateType      => "java.sql.Date"
    case _: ArrayType     => "Seq[Int]"
    case _: MapType       => "scala.collection.Map"
    case _: StructType    => "org.apache.spark.sql.Row"
    case _                => "String"
  }
}
