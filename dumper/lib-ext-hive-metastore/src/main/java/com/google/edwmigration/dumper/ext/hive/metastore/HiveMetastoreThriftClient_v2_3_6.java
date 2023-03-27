/*
 * Copyright 2022-2023 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.edwmigration.dumper.ext.hive.metastore;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.edwmigration.dumper.ext.hive.metastore.MetastoreConstants.DDL_TIME;
import static com.google.edwmigration.dumper.ext.hive.metastore.MetastoreConstants.FILES_COUNT;
import static com.google.edwmigration.dumper.ext.hive.metastore.MetastoreConstants.RAW_SIZE;
import static com.google.edwmigration.dumper.ext.hive.metastore.MetastoreConstants.ROWS_COUNT;
import static com.google.edwmigration.dumper.ext.hive.metastore.MetastoreConstants.TOTAL_SIZE;
import static com.google.edwmigration.dumper.ext.hive.metastore.utils.PartitionNameGenerator.makePartitionName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.FieldSchema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses the Thrift specification confirmed present in Hive v2.3.6, according to the Apache Hive
 * GitHub repo.
 *
 * <p>This class is not thread-safe because it wraps an underlying Thrift client which itself is not
 * thread-safe.
 */
@NotThreadSafe
public class HiveMetastoreThriftClient_v2_3_6 extends HiveMetastoreThriftClient {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreThriftClient_v2_3_6.class);

  @Nonnull
  private final com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6
          .ThriftHiveMetastore.Client
      client;

  // Deliberately not public
  /* pp */ HiveMetastoreThriftClient_v2_3_6(@Nonnull String name, @Nonnull TProtocol protocol) {
    super(name);
    this.client =
        new com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.ThriftHiveMetastore
            .Client(protocol);
  }

  @Nonnull
  @Override
  public List<? extends String> getAllDatabaseNames() throws Exception {
    return client.get_all_databases();
  }

  @Nonnull
  @Override
  public Database getDatabase(String databaseName) throws Exception {
    com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.Database database =
        client.get_database(databaseName);
    return new Database() {
      @CheckForNull
      @Override
      public String getName() {
        return database.getName();
      }

      @CheckForNull
      @Override
      public String getDescription() {
        return database.getDescription();
      }

      @CheckForNull
      @Override
      public String getOwner() {
        return database.getOwnerName();
      }

      @CheckForNull
      @Override
      public Integer getOwnerType() {
        return database.getOwnerType().getValue();
      }

      @CheckForNull
      @Override
      public String getLocation() {
        return database.getLocationUri();
      }
    };
  }

  @Nonnull
  @Override
  public List<? extends String> getAllTableNamesInDatabase(@Nonnull String databaseName)
      throws Exception {
    return client.get_all_tables(databaseName);
  }

  @Nonnull
  @Override
  public Table getTable(@Nonnull String databaseName, @Nonnull String tableName) throws Exception {
    com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.Table table =
        client.get_table(databaseName, tableName);
    Map<String, String> parameters =
        table.isSetParameters() ? table.getParameters() : new HashMap<>();

    return new Table() {
      @CheckForNull
      @Override
      public String getDatabaseName() {
        return (table.isSetDbName() ? table.getDbName() : null);
      }

      @CheckForNull
      @Override
      public String getTableName() {
        return (table.isSetTableName() ? table.getTableName() : null);
      }

      @CheckForNull
      @Override
      public String getTableType() {
        return (table.isSetTableType() ? table.getTableType() : null);
      }

      @CheckForNull
      @Override
      public Integer getCreateTime() {
        return (table.isSetCreateTime() ? table.getCreateTime() : null);
      }

      @CheckForNull
      @Override
      public Integer getLastAccessTime() {
        return (table.isSetLastAccessTime() ? table.getLastAccessTime() : null);
      }

      @CheckForNull
      @Override
      public String getOwner() {
        return (table.isSetOwner() ? table.getOwner() : null);
      }

      @CheckForNull
      @Override
      public String getOriginalViewText() {
        return (table.isSetViewOriginalText() ? table.getViewOriginalText() : null);
      }

      @CheckForNull
      @Override
      public String getExpandedViewText() {
        return (table.isSetViewExpandedText() ? table.getViewExpandedText() : null);
      }

      @CheckForNull
      @Override
      public String getLocation() {
        return (table.isSetSd() && table.getSd().isSetLocation()
            ? table.getSd().getLocation()
            : null);
      }

      @CheckForNull
      @Override
      public Integer getLastDdlTime() {
        return parameters.containsKey(DDL_TIME) ? Integer.parseInt(parameters.get(DDL_TIME)) : null;
      }

      @CheckForNull
      @Override
      public Long getTotalSize() {
        return parameters.containsKey(TOTAL_SIZE)
            ? Long.parseLong(parameters.get(TOTAL_SIZE))
            : null;
      }

      @CheckForNull
      @Override
      public Long getRawSize() {
        return parameters.containsKey(RAW_SIZE) ? Long.parseLong(parameters.get(RAW_SIZE)) : null;
      }

      @CheckForNull
      @Override
      public Long getRowsCount() {
        return parameters.containsKey(ROWS_COUNT)
            ? Long.parseLong(parameters.get(ROWS_COUNT))
            : null;
      }

      @CheckForNull
      @Override
      public Integer getFilesCount() {
        return parameters.containsKey(FILES_COUNT)
            ? Integer.parseInt(parameters.get(FILES_COUNT))
            : null;
      }

      @CheckForNull
      @Override
      public Integer getRetention() {
        return table.getRetention();
      }

      @CheckForNull
      @Override
      public Integer getBucketsCount() {
        return table.isSetSd() ? table.getSd().getNumBuckets() : null;
      }

      @CheckForNull
      @Override
      public Boolean isCompressed() {
        return table.isSetSd() && table.getSd().isCompressed();
      }

      @Nonnull
      @Override
      public List<? extends Field> getFields() throws Exception {
        // If we already have a non-null Storage Descriptor let's get the columns from it, we do one
        // less remote call and we also avoid
        // an exception "Storage schema reading not supported" due to OpenCSVSerde based tables. If
        // this is null we fall-back to calling get_fields.
        if (table.getSd() != null) {
          List<com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.FieldSchema>
              cols = table.getSd().getCols();
          if (cols != null) {
            return cols.stream().map(this::toField).collect(Collectors.toList());
          }
        }
        List<Field> out = new ArrayList<>();
        for (com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.FieldSchema field :
            client.get_fields(databaseName, tableName)) {
          out.add(toField(field));
        }
        return out;
      }

      @Nonnull
      private Field toField(
          com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.FieldSchema field) {
        return new Field() {
          @CheckForNull
          @Override
          public String getFieldName() {
            return (field.isSetName() ? field.getName() : null);
          }

          @CheckForNull
          @Override
          public String getType() {
            return (field.isSetType() ? field.getType() : null);
          }

          @CheckForNull
          @Override
          public String getComment() {
            return (field.isSetComment() ? field.getComment() : null);
          }
        };
      }

      @Nonnull
      @Override
      public List<? extends PartitionKey> getPartitionKeys() {
        List<PartitionKey> out = new ArrayList<>();
        for (com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.FieldSchema
            partitionKey : table.getPartitionKeys()) {
          out.add(
              new PartitionKey() {
                @CheckForNull
                @Override
                public String getPartitionKeyName() {
                  return (partitionKey.isSetName() ? partitionKey.getName() : null);
                }

                @CheckForNull
                @Override
                public String getType() {
                  return (partitionKey.isSetType() ? partitionKey.getType() : null);
                }

                @CheckForNull
                @Override
                public String getComment() {
                  return (partitionKey.isSetComment() ? partitionKey.getComment() : null);
                }
              });
        }
        return out;
      }

      @Nonnull
      @Override
      public List<? extends Partition> getPartitions() throws Exception {
        ImmutableList<String> partitionKeys =
            table.getPartitionKeys().stream().map(FieldSchema::getName).collect(toImmutableList());
        List<com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.Partition>
            partitionsMetadata = client.get_partitions(databaseName, tableName, (short) -1);

        return partitionsMetadata.stream()
            .map(
                partition -> {
                  String partitionName = makePartitionName(partitionKeys, partition.getValues());
                  Map<String, String> partitionParameters =
                      partition.isSetParameters() ? partition.getParameters() : ImmutableMap.of();

                  return new Partition() {
                    @Nonnull
                    @Override
                    public String getPartitionName() {
                      return partitionName;
                    }

                    @CheckForNull
                    @Override
                    public String getLocation() {
                      return (partition.isSetSd() && partition.getSd().isSetLocation()
                          ? partition.getSd().getLocation()
                          : null);
                    }

                    @CheckForNull
                    @Override
                    public Integer getCreateTime() {
                      return partition.getCreateTime();
                    }

                    @CheckForNull
                    @Override
                    public Integer getLastAccessTime() {
                      return partition.getLastAccessTime();
                    }

                    @CheckForNull
                    @Override
                    public Integer getLastDdlTime() {
                      return partitionParameters.containsKey(DDL_TIME)
                          ? Integer.parseInt(partitionParameters.get(DDL_TIME))
                          : null;
                    }

                    @CheckForNull
                    @Override
                    public Long getTotalSize() {
                      return partitionParameters.containsKey(TOTAL_SIZE)
                          ? Long.parseLong(partitionParameters.get(TOTAL_SIZE))
                          : null;
                    }

                    @CheckForNull
                    @Override
                    public Long getRawSize() {
                      return partitionParameters.containsKey(RAW_SIZE)
                          ? Long.parseLong(partitionParameters.get(RAW_SIZE))
                          : null;
                    }

                    @CheckForNull
                    @Override
                    public Long getRowsCount() {
                      return partitionParameters.containsKey(ROWS_COUNT)
                          ? Long.parseLong(partitionParameters.get(ROWS_COUNT))
                          : null;
                    }

                    @CheckForNull
                    @Override
                    public Integer getFilesCount() {
                      return partitionParameters.containsKey(FILES_COUNT)
                          ? Integer.parseInt(partitionParameters.get(FILES_COUNT))
                          : null;
                    }

                    @CheckForNull
                    @Override
                    public Boolean isCompressed() {
                      return partition.isSetSd() && partition.getSd().isCompressed();
                    }
                  };
                })
            .collect(toImmutableList());
      }
    };
  }

  @Nonnull
  @Override
  public List<? extends Function> getFunctions() throws Exception {
    com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.GetAllFunctionsResponse
        allFunctions = client.get_all_functions();
    List<Function> out = new ArrayList<>();
    for (com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.Function function :
        allFunctions.getFunctions()) {
      out.add(
          new Function() {
            @CheckForNull
            @Override
            public String getDatabaseName() {
              return (function.isSetDbName() ? function.getDbName() : null);
            }

            @CheckForNull
            @Override
            public String getFunctionName() {
              return (function.isSetFunctionName() ? function.getFunctionName() : null);
            }

            @CheckForNull
            @Override
            public String getType() {
              return (function.isSetFunctionType() ? function.getFunctionType().toString() : null);
            }

            @CheckForNull
            @Override
            public String getClassName() {
              return (function.isSetClassName() ? function.getClassName() : null);
            }

            @CheckForNull
            @Override
            public String getOwner() {
              return function.getOwnerName();
            }

            @CheckForNull
            @Override
            public Integer getOwnerType() {
              return (function.isSetOwnerType() ? function.getOwnerType().getValue() : null);
            }

            @CheckForNull
            @Override
            public Integer getCreateTime() {
              return function.getCreateTime();
            }
          });
    }
    return out;
  }

  @Override
  public void close() throws IOException {
    try {
      client.shutdown();
    } catch (TException e) {
      throw new IOException("Unable to shutdown Thrift client.", e);
    }
  }
}
