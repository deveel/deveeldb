using System;
using System.Collections;
using System.Data;

using Deveel.Data.Client;
using Deveel.Data.DbModel;

namespace Deveel.Data {
	public class DbMetadataService : IDbMetadataService {
		private Hashtable dbTypes;

		public DbDatabase GetMetadata(string connectionString) {
			DbDatabase database;
			DeveelDbConnection connection = null;

			try {
				connection = new DeveelDbConnection(connectionString);
				connection.Open();

				database = new DbDatabase(connection.Database);

				System.Data.DataTable schemata = connection.GetSchema(DeveelDbMetadataSchemaNames.Schemata, new string[1]);
				for (int i = 0; i < schemata.Rows.Count; i++) {
					DataRow row = schemata.Rows[i];

					string schemaName = row["TABLE_SCHEMA"].ToString();
					DbSchema schema = database.AddSchema(schemaName);

					GetSchema(connection, schema);
				}
			} finally {
				if (connection != null)
					connection.Dispose();
			}

			return database;
		}

		private void GetSchema(DeveelDbConnection connection, DbSchema schema) {
			string[] restrictions = new string[] { null, schema.Name, null, "TABLE", "SYSTEM TABLE", "VIEW" };
			System.Data.DataTable tables = connection.GetSchema(DeveelDbMetadataSchemaNames.Tables, restrictions);

			for (int i = 0; i < tables.Rows.Count; i++) {
				DataRow row = tables.Rows[i];

				string schemaName = row["TABLE_SCHEMA"].ToString();
				string tableName = row["TABLE_NAME"].ToString();
				string tableType = row["TABLE_TYPE"].ToString();

				//TODO: should we throw an exception?
				if (schemaName != schema.Name)
					continue;

				DbTable table = schema.AddTable(tableName, tableType);
				GetTable(connection, table);
			}
		}

		private void GetTable(DeveelDbConnection connection, DbTable table) {
			string[] restrictions = new string[] { null, table.Schema, table.Name, null };

			// privileges...
			System.Data.DataTable dataTable = connection.GetSchema(DeveelDbMetadataSchemaNames.TablePrivileges, restrictions);
			int rowCount = dataTable.Rows.Count;
			for (int i = 0; i < rowCount; i++) {
				DataRow row = dataTable.Rows[i];
				GetPrivilege(table, row);
			}

			// columns...
			dataTable = connection.GetSchema(DeveelDbMetadataSchemaNames.Columns, restrictions);
			rowCount = dataTable.Rows.Count;
			for (int i = 0; i < rowCount; i++) {
				DataRow row = dataTable.Rows[i];

				string schemaName = row["TABLE_SCHEMA"].ToString();
				string tableName = row["TABLE_NAME"].ToString();
				string columnName = row["COLUMN_NAME"].ToString();
				string columnType = row["TYPE_NAME"].ToString();

				if (schemaName != table.Schema ||
					tableName != table.Name)
					continue;

				DbDataType dataType = GetDbDataType(connection, columnType);

				DbColumn column = table.AddColumn(columnName, dataType);
				column.Size = (int) row["COLUMN_SIZE"];
				column.Scale = (int) row["DECIMAL_DIGITS"];
				column.Default = (string) row["COLUMN_DEFAULT"];

				GetColumn(connection, column);
			}

			// primary keys...
			dataTable = connection.GetSchema(DeveelDbMetadataSchemaNames.PrimaryKeys, restrictions);
			rowCount = dataTable.Rows.Count;
			for (int i = 0; i < rowCount; i++) {
				DataRow row = dataTable.Rows[i];

				string pkName = (string) row["PK_NAME"];
				DbPrimaryKey primaryKey = table.GetNamedConstraint(pkName) as DbPrimaryKey;
				//TODO: support for unnamed constraints...
				if (primaryKey == null)
					primaryKey = table.AddConstraint(pkName, DbConstraintType.PrimaryKey) as DbPrimaryKey;

				if (primaryKey == null)
					continue;

				string columnName = (string)row["COLUMN_NAME"];
				DbColumn column = table.GetColumn(columnName);
				primaryKey.AddColumn(column);
			}

			// foreign keys...
			dataTable = connection.GetSchema(DeveelDbMetadataSchemaNames.ExportedKeys, restrictions);
			rowCount = dataTable.Rows.Count;
			for (int i = 0; i < rowCount; i++) {
				DataRow row = dataTable.Rows[i];

				string fkName = (string) row["FK_NAME"];
				string pkTable = (string) row["PKTABLE_NAME"];
				string pkSchema = (string) row["PKTABLE_SCHEMA"];

				DbForeignKey foreignKey = table.GetNamedConstraint(fkName) as DbForeignKey;
				if (foreignKey == null) {
					foreignKey = table.AddConstraint(fkName, DbConstraintType.ForeignKey) as DbForeignKey;
					if (foreignKey == null)
						continue;

					foreignKey.ReferenceTable = pkTable;
					foreignKey.ReferenceSchema = pkSchema;
					foreignKey.OnDelete = (string) row["DELETE_RULE"];
					foreignKey.OnUpdate = (string) row["UPDATE_RULE"];
				}

				string fkColumnName = (string) row["FKCOLUMN_NAME"];
				string pkColumnName = (string) row["PKCOLUMN_NAME"];

				DbColumn fkColumn = table.GetColumn(fkColumnName);
				foreignKey.AddColumn(fkColumn);
				foreignKey.AddReferenceColumn(new DbColumn(pkSchema, pkTable, pkColumnName, null));
			}

			// privileges...
			dataTable = connection.GetSchema(DeveelDbMetadataSchemaNames.TablePrivileges, restrictions);
			rowCount = dataTable.Rows.Count;
			for (int i = 0; i < rowCount; i++) {
				DataRow row = dataTable.Rows[i];
				GetPrivilege(table, row);
			}
		}

		private static void GetColumn(DeveelDbConnection connection, DbColumn column) {
			string[] restrictions = new string[] { null, column.Schema, column.TableName, column.Name };
			System.Data.DataTable dataTable = connection.GetSchema(DeveelDbMetadataSchemaNames.ColumnPrivileges, restrictions);

			int rowCount = dataTable.Rows.Count;
			for (int i = 0; i < rowCount; i++) {
				DataRow row = dataTable.Rows[i];
				GetPrivilege(column, row);
			}
		}

		private static void GetPrivilege(IDbGrantableObject obj, DataRow row) {
			string privName = row["PRIVILEGE"].ToString();
			string grantor = row["GRANTOR"].ToString();
			string grantee = row["GRANTEE"].ToString();
			bool grantable = (bool)row["IS_GRANTABLE"];

			obj.AddPrivilege(privName, grantor, grantee, grantable);
		}

		private DbDataType GetDbDataType(DeveelDbConnection connection, string name) {
			IDictionary types = GetDbTypes(connection);
			return types[name] as DbDataType;
		}

		private IDictionary GetDbTypes(DeveelDbConnection connection) {
			if (dbTypes == null) {
				System.Data.DataTable dataTypes = connection.GetSchema(DeveelDbMetadataSchemaNames.DataTypes);

				int rowCount = dataTypes.Rows.Count;

				dbTypes = new Hashtable(rowCount);

				for (int i = 0; i < rowCount; i++) {
					DataRow row = dataTypes.Rows[i];

					string typeName = row["TYPE_NAME"].ToString();
					SqlType sqlType = (SqlType) row["DATA_TYPE"];

					DbDataType dataType = new DbDataType(typeName, sqlType);

					dataType.LiteralPrefix = row["LITERAL_PREFIX"].ToString();
					dataType.LiteralSuffix = row["LITERAL_SUFFIX"].ToString();
					dataType.Searchable = (bool) row["SEARCHABLE"];
					dataType.Precision = (int) row["PRECISION"];

					dbTypes[typeName] = dataType;
				}
			}

			return dbTypes;			
		}

		public IDictionary GetDbTypes(string connectionString) {
			using (DeveelDbConnection connection =  new DeveelDbConnection(connectionString)) {
				connection.Open();

				return GetDbTypes(connection);
			}
		}
	}
}