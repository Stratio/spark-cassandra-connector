package org.apache.spark.sql.cassandra

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.sources.{CreatableRelationProvider, SchemaRelationProvider, BaseRelation, RelationProvider}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Cassandra data source extends [[RelationProvider]], [[SchemaRelationProvider]] and [[CreatableRelationProvider]].
 * It's used internally by Spark SQL to create Relation for a table which specifies the Cassandra data source
 * e.g.
 *
 *      CREATE TEMPORARY TABLE tmpTable
 *      USING org.apache.spark.sql.cassandra
 *      OPTIONS (
 *       c_table "table",
 *       keyspace "keyspace",
 *       scan_type "catalyst",
 *       push_down "true",
 *       schema '{"type":"struct","fields":
 *        [{"name":"a","type":"integer","nullable":true,"metadata":{}},
 *         {"name":"b","type":"integer","nullable":true,"metadata":{}},
 *         {"name":"c","type":"integer","nullable":true,"metadata":{}}]}'
 *      )
 */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {

  import DefaultSource._

  /**
   * Creates a new relation for a cassandra table.
   *
   * c_table, keyspace, scan_type, pushdown and schema can be specified in the parameters map.
   * Specify the following scan_type:
   *   pruned           -- Selected columns return from data source connector.
   *   pruned_filtered  -- Selected columns return from data source connector.
   *                       Some filters are pushed down to data source connector.
   *                       Other filters are applied to the data returned from data source connector
   *   catalyst         -- It has the same function as pruned_filtered. The difference is
   *                       that it directly hooks up to Catalyst planner and uses Catalyst
   *                       internal methods to evaluate the filters. It also provides data type checking.
   *                       It doesn't support binary compatibility as its design doesn't consider it.
   *
   *   The default scan_type, pruned_filtered, is used if no scan_type is specified in parameters.
   *   Switch to pruned if there is any performance issue or errors when using pruned_filtered or
   *   catalyst.
   *
   * push_down must be true/false.
   *
   * schema must be in JSON format of [[StructType]].
   *
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {

    val (tableIdent, options) = parseParameters(parameters)
    sqlContext.createCassandraSourceRelation(tableIdent, options)
  }

  /**
   * Creates a new relation for a cassandra table given table, keyspace, cluster, push_down, scan_type
   * as parameters and explicitly pass schema [[StructType]] as a parameter
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType): BaseRelation = {

    val (tableIdent, options) = parseParameters(parameters)
    val optionsWithNewSchema = CassandraDataSourceOptions(options.scanType, Option(schema), options.pushDown)
    sqlContext.createCassandraSourceRelation(tableIdent, optionsWithNewSchema)

  }

  /**
   * Creates a new relation for a cassandra table given table, keyspace, cluster, push_down, scan_type
   * as parameters and explicitly pass schema [[StructType]] and data [[DataFrame]] as parameters.
   * It saves the data to the existing table depends on [[SaveMode]]
   */
  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame): BaseRelation = {

    val (tableIdent, options) = parseParameters(parameters)
    val table = sqlContext.createCassandraSourceRelation(tableIdent, options)

    mode match {
      case Append => table.insert(data, overwrite = false)
      case Overwrite => table.insert(data, overwrite = true)
      case ErrorIfExists =>
        if (table.buildScan().isEmpty()) {
          table.insert(data, overwrite = false)
        } else {
          throw new UnsupportedOperationException("Writing to a none-empty Cassandra Table is not allowed.")
        }
      case Ignore =>
        if (table.buildScan().isEmpty()) {
          table.insert(data, overwrite = false)
        }
    }

    sqlContext.createCassandraSourceRelation(tableIdent, options)
  }
}

/** Store data source options */
case class CassandraDataSourceOptions(
    scanType: String = DefaultSource.CassandraDataSourcePrunedFilteredScanTypeName,
    schema: Option[StructType] = None,
    pushDown: Boolean = true)


object DefaultSource {
  val CassandraDataSourceTableNameProperty = "c_table"
  val CassandraDataSourceKeyspaceNameProperty = "keyspace"
  val CassandraDataSourceClusterNameProperty = "cluster"
  val CassandraDataSourceScanTypeNameProperty = "scan_type"
  val CassandraDataSourUserDefinedSchemaNameProperty = "schema"
  val CassandraDataSourPushdownEnableProperty = "push_down"

  /** Default scan type */
  val CassandraDataSourcePrunedFilteredScanTypeName = "pruned_filtered"
  val CassandraDataSourcePrunedScanTypeName = "pruned"
  val CassandraDataSourceCatalystScanTypeName = "catalyst"

  /** Parse parameters map into CassandraDataSourceOptions object */
  def parseParameters(parameters: Map[String, String]) : (TableIdent, CassandraDataSourceOptions) = {
    val scanType = parameters.getOrElse(CassandraDataSourceScanTypeNameProperty,
      CassandraDataSourcePrunedFilteredScanTypeName).toLowerCase
    val tableName = parameters(CassandraDataSourceTableNameProperty)
    val keyspaceName = parameters(CassandraDataSourceKeyspaceNameProperty)
    val clusterName = parameters.get(CassandraDataSourceClusterNameProperty)
    val schema = parameters.get(CassandraDataSourUserDefinedSchemaNameProperty)
      .map(DataType.fromJson).map(_.asInstanceOf[StructType])
    val pushDown : Boolean = parameters.getOrElse(CassandraDataSourPushdownEnableProperty, "true").toBoolean

    (TableIdent(tableName, keyspaceName, clusterName),
      CassandraDataSourceOptions(scanType, schema, pushDown))
  }
}

