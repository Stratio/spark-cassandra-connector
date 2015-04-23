package org.apache.spark.sql.cassandra

import scala.reflect.ClassTag

import org.apache.spark.Logging

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLRow.CassandraSQLRowReader
import org.apache.spark.sql.catalyst.expressions.{Literal, AttributeReference, Expression, Attribute}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, ColumnDef}
import com.datastax.spark.connector.rdd.{ReadConf, ValidRDDType}
import com.datastax.spark.connector.writer.{WriteConf, SqlRowWriter}

/**
 *  Implements [[BaseRelation]] and [[InsertableRelation]].
 *  It allows to insert data into Cassandra table. It also provides some
 *  helper methods and variables for sub class to use.
 */
private[cassandra] abstract class BaseRelationImpl(
    tableIdent: TableIdent,
    connector: CassandraConnector,
    readConf: ReadConf,
    writeConf: WriteConf,
    userSpecifiedSchema: Option[StructType],
    override val sqlContext: SQLContext)
  extends BaseRelation
  with InsertableRelation
  with Serializable
  with Logging {

  protected[this] val tableDef =
    sqlContext.getCassandraSchema(tableIdent.cluster.getOrElse("default"))
      .keyspaceByName(tableIdent.keyspace).tableByName(tableIdent.table)

  protected[this] val columnNameByLowercase =
    tableDef.allColumns.map(c => (c.columnName.toLowerCase, c.columnName)).toMap

  override def schema: StructType = {
    def columnToStructField(column: ColumnDef): StructField = {
      StructField(
        column.columnName,
        ColumnDataType.catalystDataType(column.columnType, nullable = true))
    }
    userSpecifiedSchema.getOrElse(StructType(tableDef.allColumns.map(columnToStructField)))
  }

  protected[this] val baseRdd =
    sqlContext.sparkContext.cassandraTable[CassandraSQLRow](
      tableIdent.keyspace,
      tableIdent.table)(
        connector,
        readConf,
        implicitly[ClassTag[CassandraSQLRow]],
        CassandraSQLRowReader,
        implicitly[ValidRDDType[CassandraSQLRow]])

  //TODO: need add some tests for insert null
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      new CassandraConnector(sqlContext.getCassandraConnConf(tableIdent.cluster))
        .withSessionDo {
        session => session.execute(s"TRUNCATE ${quotedName(tableIdent.keyspace)}.${quotedName(tableIdent.table)}")
      }
    }

    data.rdd.saveToCassandra(
      tableIdent.keyspace,
      tableIdent.table,
      AllColumns,
      writeConf)(
        connector,
        SqlRowWriter.Factory)
  }

  def buildScan(): RDD[Row] = baseRdd.asInstanceOf[RDD[Row]]

  /** Quote name */
  private def quotedName(str: String): String = {
    "\"" + str + "\""
  }
}

/**
 * Implements [[BaseRelation]], [[InsertableRelation]] and [[PrunedScan]]
 * It allows to insert data to and scan Cassandra table. It passes the required columns to connector
 * so that only those columns return.
 */
private[cassandra] class PrunedScanRelationImpl(
    tableIdent: TableIdent,
    connector: CassandraConnector,
    readConf: ReadConf,
    writeConf: WriteConf,
    userSpecifiedSchema: Option[StructType],
    override val sqlContext: SQLContext)
  extends BaseRelationImpl(
    tableIdent,
    connector,
    readConf,
    writeConf,
    userSpecifiedSchema,
    sqlContext)
  with PrunedScan {

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val transformer = new FilterRddTransformer(requiredColumns, columnNameByLowercase, Seq.empty)
    transformer.maybeSelect(baseRdd)
  }
}

/**
 * Implements [[BaseRelation]], [[InsertableRelation]] and [[PrunedFilteredScan]]
 * It allows to insert data to and scan the table. Some filters are pushed down to connector,
 * others are combined into a combination filter which is applied to the data returned from connector.
 * [[CatalystScanRelationImpl]] may be more efficient than [[PrunedScanRelationImpl]]
 * In case there's some issue, switch to [[PrunedScan]] or [[CatalystScan]]
 *
 * This is the default Scanner to access Cassandra.
 */
private[cassandra] class PrunedFilteredScanRelationImpl(
    tableIdent: TableIdent,
    connector: CassandraConnector,
    readConf: ReadConf,
    writeConf: WriteConf,
    userSpecifiedSchema: Option[StructType],
    override val sqlContext: SQLContext)
  extends BaseRelationImpl(
    tableIdent,
    connector,
    readConf,
    writeConf,
    userSpecifiedSchema,
    sqlContext)
  with PrunedFilteredScan {

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val pushDown = new FilterPushDown(filters, tableDef)
    val pushdownFilters = pushDown.toPushDown
    val preservedFilters = pushDown.toPreserve

    logInfo(s"pushdown filters: ${pushdownFilters.toString()}")

    val dataTypeMapping: Map[String, (Int, DataType)] =
      requiredColumns.map(
        column =>
          (column,
            (requiredColumns.indexOf(column),
            ColumnDataType.catalystDataType(tableDef.columnByName(column).columnType, nullable = true))
          )
      ).toMap

    def getSchemaData(column: String, row: Row): (Any, NativeType) = {
      val (index, dataType): (Int, DataType) = dataTypeMapping(column)
      require(dataType.isPrimitive, s"${dataType.typeName} is not supported in filter.")
      (row.get(index), dataType.asInstanceOf[NativeType])
    }

    /** Evaluate filter by column value from the row */
    def translateFilter(filter: Filter): Row => Boolean = filter match {

      case EqualTo(column, v) => (a: Row)            => compareColumnValue(column, a, v) == 0
      case LessThan(column, v) => (a: Row)           => compareColumnValue(column, a, v) < 0
      case LessThanOrEqual(column, v) => (a: Row)    => compareColumnValue(column, a, v) <= 0
      case GreaterThan(column, v) => (a: Row)        => compareColumnValue(column, a, v) > 0
      case GreaterThanOrEqual(column, v) => (a: Row) => compareColumnValue(column, a, v) >= 0
      case In(column, values) => (a: Row) => val (value, dataType) = getSchemaData(column, a)
        values.toSet.contains(value)
      //TODO: need add some tests for NULL
      case IsNull(column) => (a: Row) => val (value, dataType) = getSchemaData(column, a)
        value.asInstanceOf[dataType.JvmType] == dataType.asNullable
      case IsNotNull(column) => (a: Row) => val (value, dataType) = getSchemaData(column, a)
        value.asInstanceOf[dataType.JvmType] != dataType.asNullable
      case Not(f) => (a: Row) =>    !translateFilter(f)(a)
      case And(l, r) => (a: Row) => translateFilter(l)(a) && translateFilter(r)(a)
      case Or(l, r) => (a: Row) =>  translateFilter(l)(a) || translateFilter(r)(a)
      case _ => (a: Row) => logWarning(s"Unknown $filter")
        true
    }

    def compareColumnValue(column: String, row: Row, v: Any): Int = {

      val(value, dataType) = getSchemaData(column, row)
      // TODO: Can't check the data type and null
      dataType.ordering.compare(value.asInstanceOf[dataType.JvmType], v.asInstanceOf[dataType.JvmType])
    }
    // a filter combining all other filters
    val translatedFilters = preservedFilters.map(translateFilter)
    def rowFilter(row: Row): Boolean = translatedFilters.forall(_(row))

    val transformer = new FilterRddTransformer(requiredColumns, columnNameByLowercase, pushdownFilters)
    transformer.transform(baseRdd).filter(rowFilter)
  }
}


/**
 * Implements [[BaseRelation]], [[InsertableRelation]] and [[CatalystScan]]
 * It inserts data to and scans Cassandra table. It gets filter predicates directly from Catalyst planner
 * Some predicates are pushed down to connector, others are combined into a combination filter which
 * is applied to the data returned from connector. In case there's some issue,
 * switch to [[PrunedScan]] or [[PrunedFilteredScan]]
 */
private[cassandra] class CatalystScanRelationImpl(
    tableIdent: TableIdent,
    connector: CassandraConnector,
    readConf: ReadConf,
    writeConf: WriteConf,
    userSpecifiedSchema: Option[StructType],
    override val sqlContext: SQLContext)
  extends BaseRelationImpl(
    tableIdent: TableIdent,
    connector,
    readConf,
    writeConf,
    userSpecifiedSchema,
    sqlContext)
  with CatalystScan {

  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {

    val pushDown = new PredicatePushDown(filters, tableDef)
    val pushdownFilters = pushDown.toPushDown
    val preservedFilters = pushDown.toPreserve

    logInfo(s"pushdown filters: ${pushdownFilters.toString()}")

    val dataTypeMapping: Map[String, (Int, DataType)] =
      requiredColumns.map(
        attribute =>
          (attribute.name,
            (requiredColumns.indexOf(attribute),
              ColumnDataType.catalystDataType(tableDef.columnByName(attribute.name).columnType, nullable = true))
          )
      ).toMap

    def getSchemaData(column: String, row: Row): (Any, NativeType) = {
      val (index, dataType): (Int, DataType) = dataTypeMapping(column)
      require(dataType.isPrimitive, s"${dataType.typeName} is not supported in filter.")
      (row.get(index), dataType.asInstanceOf[NativeType])
    }

    def rowFilter(row: Row): Boolean = {
      val evalAttributeReference: PartialFunction[Expression, Expression] = {
        case AttributeReference(name, _, _, _) =>
          val (value, dataType) = getSchemaData(name, row)
          Literal(value, dataType)
        case e: Expression => e
      }

      val transformedFilters = preservedFilters.map(_.transform(evalAttributeReference))

      for (filter <- transformedFilters) {
        val result = filter.eval(row)
        if (result == false || result == null)
          return false
      }
      true
    }

    val transformer = new CatalystRddTransformer(requiredColumns, columnNameByLowercase, pushdownFilters)
    transformer.transform(baseRdd).filter(rowFilter)
  }

}

object CassandraSourceRelation {

  import DefaultSource._

  def apply(tableIdent: TableIdent, sqlContext: SQLContext)(
    implicit
      connector: CassandraConnector = new CassandraConnector(sqlContext.getCassandraConnConf(None)),
      readConf: ReadConf = sqlContext.getReadConf(tableIdent),
      writeConf: WriteConf = sqlContext.getWriteConf(tableIdent),
      sourceOptions: CassandraDataSourceOptions = CassandraDataSourceOptions()) : BaseRelationImpl = {

    val scanType = sourceOptions.scanType
    val pushDown = sourceOptions.pushDown
    // Default scan type
    if (scanType == CassandraDataSourcePrunedFilteredScanTypeName && pushDown) {
      new PrunedFilteredScanRelationImpl(
        tableIdent = tableIdent,
        connector = connector,
        readConf = readConf,
        writeConf = writeConf,
        userSpecifiedSchema = sourceOptions.schema,
        sqlContext = sqlContext)
    } else if (!pushDown || scanType == CassandraDataSourcePrunedScanTypeName) {
      new PrunedScanRelationImpl(
        tableIdent = tableIdent,
        connector = connector,
        readConf = readConf,
        writeConf = writeConf,
        userSpecifiedSchema = sourceOptions.schema,
        sqlContext = sqlContext)
    } else if (scanType == CassandraDataSourceCatalystScanTypeName) {
      new CatalystScanRelationImpl(
        tableIdent = tableIdent,
        connector = connector,
        readConf = readConf,
        writeConf = writeConf,
        userSpecifiedSchema = sourceOptions.schema,
        sqlContext = sqlContext)
    } else {
      new PrunedFilteredScanRelationImpl(
        tableIdent = tableIdent,
        connector = connector,
        readConf = readConf,
        writeConf = writeConf,
        userSpecifiedSchema = sourceOptions.schema,
        sqlContext = sqlContext)
    }
  }
}