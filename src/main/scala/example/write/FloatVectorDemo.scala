package example.write

import com.google.protobuf.ByteString
import org.apache.spark.sql.types.{
  ArrayType,
  FloatType,
  LongType,
  StringType,
  StructType
}
import org.apache.spark.sql.{SaveMode, SparkSession}

import com.zilliz.spark.connector.{
  MilvusClient,
  MilvusConnectionParams,
  MilvusOption
}
import io.milvus.grpc.schema.DataType

object FloatInsertDemo extends App {
  val uri = "http://localhost:19530"
  val token = "root:Milvus"
  val collectionName = "hello_spark_float_insert"
  val filePath = "data/insert_demo/data.json"

  // 1. create collection
  val milvusClient = MilvusClient(MilvusConnectionParams(uri, token))
  val idField: String = "id_field"
  val strField: String = "str_field"
  val floatVectorField: String = "float_vector_field"
  val jsonField: String = "json_field"
  val intArrayField: String = "int_array_field"
  val arrayField: String = "array_field"

  // 2. create collection
  val collectionSchema = milvusClient.createCollectionSchema(
    name = collectionName,
    description = "hello spark milvus float insert",
    fields = Seq(
      milvusClient.createCollectionField(
        name = idField,
        isPrimary = true,
        description = "id field",
        dataType = DataType.Int64
      ),
      milvusClient.createCollectionField(
        name = strField,
        description = "string field",
        dataType = DataType.VarChar,
        typeParams = Map("max_length" -> "32")
      ),
      milvusClient.createCollectionField(
        name = floatVectorField,
        description = "float vector field",
        dataType = DataType.FloatVector,
        typeParams = Map("dim" -> "32")
      ),
      milvusClient.createCollectionField(
        name = jsonField,
        description = "json field",
        dataType = DataType.JSON
      ),
      milvusClient.createCollectionField(
        name = intArrayField,
        description = "int array field",
        dataType = DataType.Array,
        elementType = DataType.Int64,
        typeParams = Map("max_capacity" -> "5")
      ),
      milvusClient.createCollectionField(
        name = arrayField,
        description = "array field",
        dataType = DataType.Array,
        elementType = DataType.VarChar,
        typeParams = Map("max_capacity" -> "5", "max_length" -> "32")
      )
    )
  )
  val createStatus = milvusClient.createCollection(
    collectionName = collectionName,
    schema = collectionSchema
  )
  assert(
    createStatus.isSuccess,
    s"create collection ${collectionName} failed: ${createStatus.failed.getOrElse("Unknown error")}"
  )

  // 3. insert data
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("InsertDemo")
    .getOrCreate()
  val df = spark.read
    .schema(
      new StructType()
        .add(idField, LongType)
        .add(strField, StringType)
        .add(floatVectorField, ArrayType(FloatType), false)
        .add(jsonField, StringType, false)
        .add(intArrayField, ArrayType(LongType), false)
        .add(arrayField, ArrayType(StringType), false)
    )
    .json(filePath)
  val milvusOption = Map(
    MilvusOption.MilvusUri -> uri,
    MilvusOption.MilvusToken -> token,
    MilvusOption.MilvusCollectionName -> collectionName
  )
  df.write
    .options(milvusOption)
    .format("milvus")
    .mode(SaveMode.Append)
    .save()
  println("insert data success")

  spark.stop()
  milvusClient.close()
}
