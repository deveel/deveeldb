using System;
using System.Data;
using System.Text;

using Deveel.Data.Client;

namespace Deveel.Data.Shell {
	class DatabaseMetaData {

		/**
		 * The Connection object associated with this meta data.
		 */
		private DbConnection connection;

		/**
		 * The name and version of the database we are connected to.
		 */
		private String database_name, database_version;

		/**
		 * Constructor.
		 */
		internal DatabaseMetaData(DbConnection connection) {
			this.connection = connection;
		}

		/**
		 * Queries product information about the database we are connected to.
		 */
		private void queryProductInformation() {
			if (database_name == null ||
				database_version == null) {
				IDbCommand command = connection.CreateCommand();
				command.CommandText = "SHOW PRODUCT";
				IDataReader result = command.ExecuteReader();
				result.Read();
				database_name = result.GetString(result.GetOrdinal("name"));
				database_version = result.GetString(result.GetOrdinal("version"));
				result.Close();
			}
		}



		//----------------------------------------------------------------------
		// First, a variety of minor information about the target database.

		public String getUserName() {
			IDbCommand statement = connection.CreateCommand();
			statement.CommandText = "SELECT USER()";
			IDataReader result_set = statement.ExecuteReader();
			result_set.Read();
			String username = result_set.GetString(0);
			result_set.Close();
			return username;
		}

		public bool isReadOnly() {
			IDbCommand statement = connection.CreateCommand();
			statement.CommandText = " SELECT * FROM SYS_INFO.sUSRDatabaseStatistics " +
									"  WHERE \"stat_name\" = 'DatabaseSystem.read_only' ";

			IDataReader result_set = statement.ExecuteReader();
			bool read_only = result_set.Read();
			result_set.Close();
			return read_only;
		}


		public String getDatabaseProductName() {
			queryProductInformation();
			return database_name;
		}

		public String getDatabaseProductVersion() {
			queryProductInformation();
			return database_version;
		}


		//----------------------------------------------------------------------
		// The following group of methods exposes various limitations
		// based on the target database with the current driver.
		// Unless otherwise specified, a result of zero means there is no
		// limit, or the limit is not known.

		public int getMaxBinaryLiteralLength() {
			// No binary literals yet,
			return 0;
		}

		public int getMaxCharLiteralLength() {
			// This is an educated guess...
			return 32768;
		}

		public int getMaxColumnNameLength() {
			// Need to work out this limitation for real.  There may be no limit.
			return 256;
		}

		public int getMaxColumnsInIndex() {
			// No explicit indexing syntax,
			return 1;
		}

		public int getMaxColumnsInOrderBy() {
			// The limit is determined by number of columns in select.
			return getMaxColumnsInSelect();
		}

		public int getMaxColumnsInSelect() {
			// Probably limited only by resources...
			return 4096;
		}

		public int getMaxColumnsInTable() {
			// Probably limited only by resources...
			return 4096;
		}

		public int getMaxRowSize() {
			// Only limited by resources,
			// Returning 16MB here.
			return 16 * 1024 * 1024;
		}

		//----------------------------------------------------------------------

		private static void SetParameter(IDbCommand command, object value) {
			IDbDataParameter parameter = command.CreateParameter();
			parameter.Value = value;
			command.Parameters.Add(parameter);
		}

		public IDataReader getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) {

			if (tableNamePattern == null) {
				tableNamePattern = "%";
			}
			if (schemaPattern == null) {
				schemaPattern = "%";
			}

			// The 'types' argument
			String type_part = "";
			int type_size = 0;
			if (types != null && types.Length > 0) {
				StringBuilder buf = new StringBuilder();
				buf.Append("      AND \"TABLE_TYPE\" IN ( ");
				for (int i = 0; i < types.Length - 1; ++i) {
					buf.Append("?, ");
				}
				buf.Append("? ) \n");
				type_size = types.Length;
				type_part = buf.ToString();
			}

			// Create the statement

			IDbCommand stmt = connection.CreateCommand();
			stmt.CommandText =
			  "   SELECT * \n" +
			  "     FROM \"INFORMATION_SCHEMA.Tables\" \n" +
			  "    WHERE \"TABLE_SCHEM\" LIKE ? \n" +
			  "      AND \"TABLE_NAME\" LIKE ? \n" +
			  type_part +
			  " ORDER BY \"TABLE_TYPE\", \"TABLE_SCHEM\", \"TABLE_NAME\" \n"
		  ;
			SetParameter(stmt, schemaPattern);
			SetParameter(stmt, tableNamePattern);
			if (type_size > 0) {
				for (int i = 0; i < type_size; ++i) {
					SetParameter(stmt, types[i]);
				}
			}

			return stmt.ExecuteReader();

		}

		public IDataReader getSchemas() {
			IDbCommand statement = connection.CreateCommand();
			statement.CommandText =
			 "    SELECT * \n" +
			 "      FROM INFORMATION_SCHEMA.Schemata \n" +
			 "  ORDER BY \"TABLE_SCHEM\" "
		  ;
			return statement.ExecuteReader();
		}

		/*
		public IDataReader getCatalogs() {
			IDbCommand statement = connection.CreateCommand();
			statement.CommandText = "SHOW JDBC_CATALOGS";
			return statement.ExecuteReader();
		}

		public IDataReader getTableTypes() {
			IDbCommand statement = connection.CreateCommand();
			statement.CommandText = "SHOW JDBC_TABLE_TYPES";
			return statement.ExecuteReader();
		}
		*/

		public IDataReader getColumns(String catalog, String schemaPattern,
		  String tableNamePattern, String columnNamePattern) {

			if (tableNamePattern == null) {
				tableNamePattern = "%";
			}
			if (schemaPattern == null) {
				schemaPattern = "%";
			}
			if (columnNamePattern == null) {
				columnNamePattern = "%";
			}

			IDbCommand statement = connection.CreateCommand();
			statement.CommandText =
				"  SELECT * \n" +
				"    FROM INFORMATION_SCHEMA.Columns \n" +
				"   WHERE \"TABLE_SCHEM\" LIKE ? \n" +
				"     AND \"TABLE_NAME\" LIKE ? \n" +
				"     AND \"COLUMN_NAME\" LIKE ? \n" +
				"ORDER BY \"TABLE_SCHEM\", \"TABLE_NAME\", \"ORDINAL_POSITION\"";

			SetParameter(statement, schemaPattern);
			SetParameter(statement, tableNamePattern);
			SetParameter(statement, columnNamePattern);
			statement.Prepare();

			return statement.ExecuteReader();
		}

		public IDataReader getColumnPrivileges(String catalog, String schema,
			String table, String columnNamePattern) {

			if (columnNamePattern == null) {
				columnNamePattern = "%";
			}

			IDbCommand statement = connection.CreateCommand();
			statement.CommandText =
				"   SELECT * FROM INFORMATION_SCHEMA.Column_Privileges \n" +
				"    WHERE (? IS NOT NULL OR \"TABLE_SCHEMA\" = ? ) \n" +
				"      AND (? IS NOT NULL OR \"TABLE_NAME\" = ? ) \n" +
				"      AND \"COLUMN_NAME\" LIKE ? \n" +
				" ORDER BY \"COLUMN_NAME\", \"PRIVILEGE\" ";

			SetParameter(statement, schema);
			SetParameter(statement, schema);
			SetParameter(statement, table);
			SetParameter(statement, table);
			SetParameter(statement, columnNamePattern);

			return statement.ExecuteReader();
		}

		public IDataReader getTablePrivileges(String catalog, String schemaPattern,
			  String tableNamePattern) {

			if (schemaPattern == null) {
				schemaPattern = "%";
			}
			if (tableNamePattern == null) {
				tableNamePattern = "%";
			}

			IDbCommand statement = connection.CreateCommand();
			statement.CommandText =
			"   SELECT * FROM INFORMATION_SCHEMA.Table_Privileges \n" +
		"    WHERE \"TABLE_SCHEM\" LIKE ? \n" +
		"      AND \"TABLE_NAME\" LIKE ? \n" +
		" ORDER BY \"TABLE_SCHEM\", \"TABLE_NAME\", \"PRIVILEGE\" ";

			SetParameter(statement, schemaPattern);
			SetParameter(statement, tableNamePattern);

			return statement.ExecuteReader();
		}

		public IDataReader getBestRowIdentifier(String catalog, String schema,
		  String table, int scope, bool nullable) {

			IDbCommand statement = connection.CreateCommand();
						  statement.CommandText = "SHOW JDBC_BEST_ROW_IDENTIFIER ( ?, ?, ?, ?, ? )";
			SetParameter(statement, catalog);
			SetParameter(statement, schema);
			SetParameter(statement, table);
			SetParameter(statement, scope);
			SetParameter(statement, nullable);

			return statement.ExecuteReader();
		}

		public IDataReader getVersionColumns(String catalog, String schema,
			  String table) {

			IDbCommand statement = connection.CreateCommand();
								   statement.CommandText = "SHOW JDBC_VERSION_COLUMNS ( ?, ?, ? )";
			SetParameter(statement, catalog);
			SetParameter(statement, schema);
			SetParameter(statement, table);

			return statement.ExecuteReader();
		}

		public IDataReader getPrimaryKeys(String catalog, String schema,
			  String table) {

			IDbCommand stmt = connection.CreateCommand();
			stmt.CommandText =
		"   SELECT * \n" +
		"     FROM SYS_JDBC.PrimaryKeys \n" +
		"    WHERE ( ? IS NULL OR \"TABLE_SCHEM\" = ? ) \n" +
		"      AND \"TABLE_NAME\" = ? \n" +
		" ORDER BY \"COLUMN_NAME\"";
			SetParameter(stmt, schema);
			SetParameter(stmt, schema);
			SetParameter(stmt, table);

			return stmt.ExecuteReader();

		}

		public IDataReader getImportedKeys(String catalog, String schema,
			  String table) {

			IDbCommand stmt = connection.CreateCommand();
			stmt.CommandText =
		"   SELECT * FROM SYS_JDBC.ImportedKeys \n" +
		"    WHERE ( ? IS NULL OR \"FKTABLE_SCHEM\" = ? )\n" +
		"      AND \"FKTABLE_NAME\" = ? \n" +
		"ORDER BY \"FKTABLE_SCHEM\", \"FKTABLE_NAME\", \"KEY_SEQ\"";
			SetParameter(stmt, schema);
			SetParameter(stmt, schema);
			SetParameter(stmt, table);

			return stmt.ExecuteReader();
		}

		public IDataReader getExportedKeys(String catalog, String schema,
			  String table) {

			IDbCommand stmt = connection.CreateCommand();
			stmt.CommandText = 
		"   SELECT * FROM SYS_JDBC.ImportedKeys \n" +
		"    WHERE ( ? IS NULL OR \"PKTABLE_SCHEM\" = ? ) \n" +
		"      AND \"PKTABLE_NAME\" = ? \n" +
		"ORDER BY \"FKTABLE_SCHEM\", \"FKTABLE_NAME\", \"KEY_SEQ\"";
			SetParameter(stmt, schema);
			SetParameter(stmt, schema);
			SetParameter(stmt, table);

			return stmt.ExecuteReader();
		}

		public IDataReader getCrossReference(
		  String primaryCatalog, String primarySchema, String primaryTable,
		  String foreignCatalog, String foreignSchema, String foreignTable
		  ) {

			IDbCommand stmt = connection.CreateCommand();
		stmt.CommandText = "   SELECT * FROM SYS_JDBC.ImportedKeys \n" +
		"   WHERE ( ? IS NULL OR \"PKTABLE_SCHEM\" = ? )\n" +
		"     AND \"PKTABLE_NAME\" = ?\n" +
		"     AND ( ? IS NULL OR \"FKTABLE_SCHEM\" = ? )\n" +
		"     AND \"FKTABLE_NAME\" = ?\n" +
		"ORDER BY \"FKTABLE_SCHEM\", \"FKTABLE_NAME\", \"KEY_SEQ\"\n";
			SetParameter(stmt, primarySchema);
			SetParameter(stmt, primarySchema);
			SetParameter(stmt, primaryTable);
			SetParameter(stmt, foreignSchema);
			SetParameter(stmt, foreignSchema);
			SetParameter(stmt, foreignTable);

			return stmt.ExecuteReader();
		}

		public IDataReader getTypeInfo() {
			IDbCommand command = connection.CreateCommand();
			command.CommandText = "SELECT * FROM SYSTEM.sUSRSQLTypeInfo";
			return command.ExecuteReader();
		}

		public IDataReader getIndexInfo(String catalog, String schema, String table,
			bool unique, bool approximate) {

			IDbCommand statement = connection.CreateCommand();
			statement.CommandText = "SHOW JDBC_INDEX_INFO ( ?, ?, ?, ?, ? )";
			SetParameter(statement, catalog);
			SetParameter(statement, schema);
			SetParameter(statement, table);
			SetParameter(statement, unique);
			SetParameter(statement, approximate);
			statement.Prepare();
			return statement.ExecuteReader();
		}


		public IDataReader getUDTs(String catalog, String schemaPattern, String typeNamePattern, SQLTypes[] types) {
			String where_clause = "true";
			if (types != null) {
				for (int i = 0; i < types.Length; ++i) {

					SQLTypes t = types[i];
					String tstr = "OBJECT";
					if (t == SQLTypes.STRUCT) {
						tstr = "STRUCT";
					} else if (t == SQLTypes.DISTINCT) {
						tstr = "DISTINCT";
					}

					if (i != 0) {
						where_clause += " AND";
					}
					//TODO: where_clause += " DATA_TYPE = '" + DatabaseConnection.Quote(tstr) + "'";
					where_clause += " DATA_TYPE = '" + tstr + "'";
				}
			}

			IDbCommand statement = connection.CreateCommand();
			statement.CommandText = "SHOW JDBC_UDTS ( ?, ?, ? ) WHERE " + where_clause;
			SetParameter(statement, catalog);
			SetParameter(statement, schemaPattern);
			SetParameter(statement, typeNamePattern);
			statement.Prepare();
			return statement.ExecuteReader();
		}
	}
}