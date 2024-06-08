package com.yltech;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;

import org.apache.iceberg.types.Types;

import org.apache.iceberg.catalog.TableIdentifier;

// iceberg hadoop tables
import org.apache.iceberg.Table;

//


public class IcebergUtil {
    public static Catalog createHadoopCatalog() {
        Configuration conf = new Configuration();
        String warehousePath = "hdfs://localhost:9000/iceberg";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        return catalog;
    }

    public static Schema createSchema() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "sec_id", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "bid", Types.DoubleType.get()),
                Types.NestedField.required(4, "ask", Types.DoubleType.get()),
                Types.NestedField.required(5, "trade", Types.DoubleType.get())
        );
        return schema;
    }

    public static PartitionSpec createPartitionSpec(Schema schema) {
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .day("event_time")
                .bucket("sec_id", 10)
                .build();
        return spec;
    }

    public static Table createTable(Catalog catalog, Schema schema, PartitionSpec spec, TableIdentifier tableId) {
        TableIdentifier name = tableId;
        Table table = catalog.createTable(name, schema, spec);

        // or to load an existing table, use the following line
        // Table table = catalog.loadTable(name);

        return table;
    }

    public static void appendFile(Table table, DataFile data) {
        Transaction t = table.newTransaction();

        t.newAppend().appendFile(data).commit();

        t.commitTransaction();
    }
}
