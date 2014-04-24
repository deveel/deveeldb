// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;

using SysDataTable = System.Data.DataTable;
using SysDataRow = System.Data.DataRow;

namespace Deveel.Data.Client {
	internal class DatabaseMetadata {
		public DatabaseMetadata(DeveelDbConnection connection) {
			this.connection = connection;
		}

		private readonly DeveelDbConnection connection;

		public SysDataTable GetSchemata(string[] restrictions) {
			if (restrictions == null)
				throw new ArgumentException();

			if (restrictions.Length > 1)
				throw new ArgumentException();

			string schema = restrictions[0];
			if (schema == null)
				schema = "%";

			SysDataTable dataTable = new SysDataTable("Schemata");

			dataTable.Columns.Add("TABLE_SCHEMA");
			dataTable.Columns.Add("TABLE_CATALOG");

			DeveelDbCommand command = connection.CreateCommand("   SELECT * \n" +
															   "     FROM \"INFORMATION_SCHEMA.TABLES\" \n" +
															   "    WHERE \"TABLE_SCHEMA\" LIKE ? \n" +
															   " ORDER BY \"TABLE_SCHEMA\" \n");

			command.Parameters.Add(schema);
			command.Prepare();

			using (DeveelDbDataReader reader = command.ExecuteReader()) {
				SysDataRow row = dataTable.NewRow();
				row["TABLE_SCHEMA"] = reader.GetString(0);
				row["TABLE_CATALOG"] = reader.GetString(1);
				dataTable.Rows.Add(row);
			}

			return dataTable;
		}

		public SysDataTable GetTables(string[] restrictions) {
			if (restrictions == null)
				throw new ArgumentNullException("restrictions");
			if (restrictions.Length < 3)
				throw new ArgumentException();

			//TODO: still not officially supported...
			string catalog = restrictions[0];
			string schema = restrictions[1];
			string table = restrictions[2];

			string[] types = new string[restrictions.Length - 3];
			Array.Copy(restrictions, 3, types, 0, types.Length);

			SysDataTable dataTable = new SysDataTable("Tables");
			dataTable.Columns.Add("TABLE_CATALOG");
			dataTable.Columns.Add("TABLE_SCHEMA");
			dataTable.Columns.Add("TABLE_NAME");
			dataTable.Columns.Add("TABLE_TYPE");
			dataTable.Columns.Add("REMARKS");
			dataTable.Columns.Add("TYPE_CATALOG");
			dataTable.Columns.Add("TYPE_SCHEMA");
			dataTable.Columns.Add("TYPE_NAME");
			dataTable.Columns.Add("SELF_REFERENCING_COL_NAME");
			dataTable.Columns.Add("REF_GENERATION");

			if (table == null)
				table = "%";
			if (schema == null)
				schema = "%";

			// The 'types' argument
			String typePart = "";
			int typeSize = 0;
			if (types.Length > 0) {
				StringBuilder buf = new StringBuilder();
				buf.Append("      AND \"TABLE_TYPE\" IN ( ");
				for (int i = 0; i < types.Length - 1; ++i) {
					buf.Append("?, ");
				}
				buf.Append("? ) \n");
				typeSize = types.Length;
				typePart = buf.ToString();
			}

			// Create the statement

			DeveelDbCommand command = connection.CreateCommand("   SELECT * \n" +
			                                                   "     FROM \"INFORMATION_SCHEMA.TABLES\" \n" +
			                                                   "    WHERE \"TABLE_SCHEMA\" LIKE ? \n" +
			                                                   "      AND \"TABLE_NAME\" LIKE ? \n" +
			                                                   typePart +
			                                                   " ORDER BY \"TABLE_TYPE\", \"TABLE_SCHEMA\", \"TABLE_NAME\" \n");
			command.Parameters.Add(schema);
			command.Parameters.Add(table);
			if (typeSize > 0) {
				for (int i = 0; i < typeSize; ++i)
					command.Parameters.Add(types[i]);
			}

			command.Prepare();

			using (DeveelDbDataReader reader = command.ExecuteReader()) {
				while (reader.Read()) {
					SysDataRow row = dataTable.NewRow();
					row["TABLE_CATALOG"] = reader.GetString(0);
					row["TABLE_SCHEMA"] = reader.GetString(1);
					row["TABLE_NAME"] = reader.GetString(2);
					row["TABLE_TYPE"] = reader.GetString(3);
					row["REMARKS"] = reader.GetString(4);
					// the other columns are always NULL so it's useless to read...

					dataTable.Rows.Add(row);
				}
			}

			return dataTable;
		}

		public SysDataTable GetColumns(string[] restrictions) {
			if (restrictions == null)
				throw new ArgumentNullException("restrictions");
			if (restrictions.Length < 4)
				throw new ArgumentException("COLUMNS collection requires at least 4 arguments.");

			string catalog = restrictions[0];
			string schema = restrictions[1];
			string table = restrictions[2];
			string column = restrictions[3];

			if (table == null)
				table = "%";
			if (schema == null)
				schema = "%";
			if (column == null)
				column = "%";

			SysDataTable dataTable = new SysDataTable("Columns");
			dataTable.Columns.Add("TABLE_CATALOG");
			dataTable.Columns.Add("TABLE_SCHEMA");
			dataTable.Columns.Add("TABLE_NAME");
			dataTable.Columns.Add("COLUMN_NAME");
			dataTable.Columns.Add("DATA_TYPE", typeof(int));
			dataTable.Columns.Add("TYPE_NAME");
			dataTable.Columns.Add("COLUMN_SIZE", typeof(int));
			dataTable.Columns.Add("BUFFER_LENGTH", typeof(int));
			dataTable.Columns.Add("DECIMAL_DIGITS", typeof(int));
			dataTable.Columns.Add("NUM_PREC_RADIX", typeof(int));
			dataTable.Columns.Add("NULLABLE", typeof(bool));
			dataTable.Columns.Add("REMARKS");
			dataTable.Columns.Add("COLUMN_DEFAULT");
			dataTable.Columns.Add("SQL_DATA_TYPE");
			dataTable.Columns.Add("SQL_DATETIME_SUB");
			dataTable.Columns.Add("CHAR_OCTET_LENGTH", typeof(int));
			dataTable.Columns.Add("ORDINAL_POSITION", typeof(int));
			dataTable.Columns.Add("IS_NULLABLE", typeof (bool));

			DeveelDbCommand command = connection.CreateCommand("  SELECT * \n" +
			                                                   "    FROM INFORMATION_SCHEMA.COLUMNS \n" +
			                                                   "   WHERE \"TABLE_SCHEMA\" LIKE ? \n" +
			                                                   "     AND \"TABLE_NAME\" LIKE ? \n" +
			                                                   "     AND \"COLUMN_NAME\" LIKE ? \n" +
			                                                   "ORDER BY \"TABLE_SCHEMA\", \"TABLE_NAME\", \"ORDINAL_POSITION\"");
			command.Parameters.Add(schema);
			command.Parameters.Add(table);
			command.Parameters.Add(column);
			command.Prepare();

			using (DeveelDbDataReader reader = command.ExecuteReader()) {
				while (reader.Read()) {
					SysDataRow row = dataTable.NewRow();
					row["TABLE_CATALOG"] = reader.GetString(0);
					row["TABLE_SCHEMA"] = reader.GetString(1);
					row["TABLE_NAME"] = reader.GetString(2);
					row["COLUMN_NAME"] = reader.GetString(3);
					row["DATA_TYPE"] = reader.GetInt32(4);
					row["TYPE_NAME"] = reader.GetString(5);
					row["COLUMN_SIZE"] = reader.GetInt32(6);
					row["BUFFER_LENGTH"] = reader.GetInt32(7);
					row["DECIMAL_DIGITS"] = reader.GetInt32(8);
					row["NUM_PREC_RADIX"] = reader.GetInt32(9);
					row["NULLABLE"] = reader.GetBoolean(10);
					row["REMARKS"] = reader.GetString(11);
					row["COLUMN_DEFAULT"] = reader.GetString(12);
					row["SQL_DATA_TYPE"] = reader.GetString(13);
					row["SQL_DATETIME_SUB"] = reader.GetString(14);
					row["CHAR_OCTET_LENGTH"] = reader.GetInt32(15);
					row["ORDINAL_POSITION"] = reader.GetInt32(16);
					row["IS_NULLABLE"] = reader.GetString(17) == "YES";
					dataTable.Rows.Add(row);
				}
			}

			return dataTable;
		}

		public SysDataTable GetColumnPrivileges(string[] restrictions) {
			if (restrictions == null)
				throw new ArgumentNullException("restrictions");
			if (restrictions.Length < 3)
				throw new ArgumentException();

			string catalog = restrictions[0];
			string schema = restrictions[1];
			string table = restrictions[2];
			string column = restrictions[3];

			if (table == null)
				throw new ArgumentException("The table name must be specified.");

			if (column == null)
				column = "%";

			SysDataTable dataTable = new SysDataTable("Column_Privileges");

			dataTable.Columns.Add("TABLE_CATALOG");
			dataTable.Columns.Add("TABLE_SCHEMA");
			dataTable.Columns.Add("TABLE_NAME");
			dataTable.Columns.Add("COLUMN_NAME");
			dataTable.Columns.Add("GRANTOR");
			dataTable.Columns.Add("GRANTEE");
			dataTable.Columns.Add("PRIVILEGE");
			dataTable.Columns.Add("IS_GRANTABLE", typeof(bool));

			DeveelDbCommand command = connection.CreateCommand("   SELECT * FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES \n" +
			                                                   "    WHERE (? IS NULL OR \"TABLE_SCHEMA\" = ? ) \n" +
			                                                   "      AND (? IS NOT NULL AND \"TABLE_NAME\" = ? ) \n" +
			                                                   "      AND \"COLUMN_NAME\" LIKE ? \n" +
			                                                   " ORDER BY \"COLUMN_NAME\", \"PRIVILEGE\" ");
			command.Parameters.Add(schema);
			command.Parameters.Add(schema);
			command.Parameters.Add(table);
			command.Parameters.Add(table);
			command.Parameters.Add(column);

			command.Prepare();

			using (DeveelDbDataReader reader = command.ExecuteReader()) {
				while (reader.Read()) {
					SysDataRow row = dataTable.NewRow();
					row["TABLE_CATALOG"] = reader.GetString(0);
					row["TABLE_SCHEMA"] = reader.GetString(1);
					row["TABLE_NAME"] = reader.GetString(2);
					row["COLUMN_NAME"] = reader.GetString(3);
					row["GRANTOR"] = reader.GetString(4);
					row["GRANTEE"] = reader.GetString(5);
					row["PRIVILEGE"] = reader.GetString(6);
					row["IS_GRANTABLE"] = reader.GetString(7) == "YES";
					dataTable.Rows.Add(row);
				}
			}

			return dataTable;
		}

		public SysDataTable GetTablePrivileges(string[] restrictions) {
			if (restrictions == null)
				throw new ArgumentNullException("restrictions");
			if (restrictions.Length < 3)
				throw new ArgumentException();

			string catalog = restrictions[0];
			string schema = restrictions[1];
			string table = restrictions[2];

			if (schema == null)
				schema = "%";
			if (table == null)
				table = "%";

			SysDataTable dataTable = new SysDataTable("TablePrivileges");
			dataTable.Columns.Add("TABLE_CATALOG");
			dataTable.Columns.Add("TABLE_SCHEMA");
			dataTable.Columns.Add("TABLE_NAME");
			dataTable.Columns.Add("PRIVILEGE");
			dataTable.Columns.Add("GRANTOR");
			dataTable.Columns.Add("GRANTEE");
			dataTable.Columns.Add("IS_GRANTABLE", typeof(bool));

			DeveelDbCommand command = connection.CreateCommand("   SELECT * FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES \n" +
			                                                   "    WHERE \"TABLE_SCHEMA\" LIKE ? \n" +
			                                                   "      AND \"TABLE_NAME\" LIKE ? \n" +
			                                                   " ORDER BY \"TABLE_SCHEMA\", \"TABLE_NAME\", \"PRIVILEGE\" ");

			command.Parameters.Add(schema);
			command.Parameters.Add(table);
			command.Prepare();

			using (DeveelDbDataReader reader = command.ExecuteReader()) {
				while (reader.Read()) {
					SysDataRow row = dataTable.NewRow();
					row["TABLE_CATALOG"] = reader.GetString(0);
					row["TABLE_SCHEMA"] = reader.GetString(1);
					row["TABLE_NAME"] = reader.GetString(2);
					row["GRANTOR"] = reader.GetString(3);
					row["GRANTEE"] = reader.GetString(4);
					row["PRIVILEGE"] = reader.GetString(5);
					row["IS_GRANTABLE"] = reader.GetString(6) == "YES";
					dataTable.Rows.Add(row);
				}
			}

			return dataTable;
		}

		public SysDataTable GetPrimaryKeys(string[] restrictions) {
			if (restrictions == null)
				throw new ArgumentNullException("restrictions");
			if (restrictions.Length < 3)
				throw new ArgumentException();

			string catalog = restrictions[0];
			string schema = restrictions[1];
			string table = restrictions[2];

			SysDataTable dataTable = new SysDataTable("PrimaryKeys");
			dataTable.Columns.Add("TABLE_CATALOG");
			dataTable.Columns.Add("TABLE_SCHEMA");
			dataTable.Columns.Add("TABLE_NAME");
			dataTable.Columns.Add("COLUMN_NAME");
			dataTable.Columns.Add("KEY_SEQ", typeof(int));
			dataTable.Columns.Add("PK_NAME");

			DeveelDbCommand command = connection.CreateCommand("   SELECT * \n" +
			                                                   "     FROM INFORMATION_SCHEMA.PrimaryKeys \n" +
			                                                   "    WHERE ( ? IS NULL OR \"TABLE_SCHEMA\" = ? ) \n" +
			                                                   "      AND \"TABLE_NAME\" = ? \n" +
			                                                   " ORDER BY \"COLUMN_NAME\"");

			command.Parameters.Add(schema);
			command.Parameters.Add(schema);
			command.Parameters.Add(table);

			command.Prepare();

			using (DeveelDbDataReader reader = command.ExecuteReader()) {
				SysDataRow row = dataTable.NewRow();
				row["TABLE_CATALOG"] = reader.GetString(0);
				row["TABLE_SCHEMA"] = reader.GetString(1);
				row["TABLE_NAME"] = reader.GetString(2);
				row["COLUMN_NAME"] = reader.GetString(3);
				row["KEY_SEQ"] = reader.GetInt32(4);
				row["PK_NAME"] = reader.GetString(5);
				dataTable.Rows.Add(row);
			}

			return dataTable;
		}

		public SysDataTable GetImportedKeys(string[] restrictions) {
			if (restrictions == null)
				throw new ArgumentNullException("restrictions");
			if (restrictions.Length < 3)
				throw new ArgumentException();

			string catalog = restrictions[0];
			string schema = restrictions[1];
			string table = restrictions[2];

			var dataTable = new SysDataTable("ImportedKey");
			dataTable.Columns.Add("PKTABLE_CATALOG");
			dataTable.Columns.Add("PKTABLE_SCHEMA");
			dataTable.Columns.Add("PKTABLE_NAME");
			dataTable.Columns.Add("PKCOLUMN_NAME");
			dataTable.Columns.Add("FKTABLE_CATALOG");
			dataTable.Columns.Add("FKTABLE_SCHEMA");
			dataTable.Columns.Add("FKTABLE_NAME");
			dataTable.Columns.Add("FKCOLUMN_NAME");
			dataTable.Columns.Add("KEY_SEQ");
			dataTable.Columns.Add("UPDATE_RULE");
			dataTable.Columns.Add("DELETE_RULE");
			dataTable.Columns.Add("FK_NAME");
			dataTable.Columns.Add("PK_NAME");
			dataTable.Columns.Add("DEFERRABILITY");

			DeveelDbCommand command = connection.CreateCommand("   SELECT * FROM INFORMATION_SCHEMA.imported_keys \n" +
			                                                   "    WHERE ( ? IS NULL OR \"FKTABLE_SCHEMA\" = ? )\n" +
			                                                   "      AND \"FKTABLE_NAME\" = ? \n" +
			                                                   " ORDER BY \"FKTABLE_SCHEMA\", \"FKTABLE_NAME\", \"KEY_SEQ\"");

			command.Parameters.Add(schema);
			command.Parameters.Add(schema);
			command.Parameters.Add(table);

			command.Prepare();

			using (DeveelDbDataReader reader = command.ExecuteReader()) {
				if (reader.HasRows) {
					while (reader.Read()) {
						var row = dataTable.NewRow();
						row["PKTABLE_CATALOG"] = reader.GetString(0);
						row["PKTABLE_SCHEMA"] = reader.GetString(1);
						row["PKTABLE_NAME"] = reader.GetString(2);
						row["PKCOLUMN_NAME"] = reader.GetString(3);
						row["FKTABLE_CATALOG"] = reader.GetString(4);
						row["FKTABLE_SCHEMA"] = reader.GetString(5);
						row["FKTABLE_NAME"] = reader.GetString(6);
						row["FKCOLUMN_NAME"] = reader.GetString(7);
						row["KEY_SEQ"] = reader.GetInt32(8);
						row["UPDATE_RULE"] = reader.GetString(9);
						row["DELETE_RULE"] = reader.GetString(10);
						row["FK_NAME"] = reader.GetString(11);
						row["PK_NAME"] = reader.GetString(12);
						row["DEFERRABILITY"] = reader.GetInt32(13);
						dataTable.Rows.Add(row);
					}
				}
			}

			return dataTable;
		}

		public SysDataTable GetExportedKeys(string[] restrictions) {
			if (restrictions == null)
				throw new ArgumentNullException("restrictions");
			if (restrictions.Length < 3)
				throw new ArgumentException();

			string catalog = restrictions[0];
			string schema = restrictions[1];
			string table = restrictions[2];

			SysDataTable dataTable = new SysDataTable("ExportedKey");
			dataTable.Columns.Add("PKTABLE_CATALOG");
			dataTable.Columns.Add("PKTABLE_SCHEMA");
			dataTable.Columns.Add("PKTABLE_NAME");
			dataTable.Columns.Add("PKCOLUMN_NAME");
			dataTable.Columns.Add("FKTABLE_CATALOG");
			dataTable.Columns.Add("FKTABLE_SCHEMA");
			dataTable.Columns.Add("FKTABLE_NAME");
			dataTable.Columns.Add("FKCOLUMN_NAME");
			dataTable.Columns.Add("KEY_SEQ");
			dataTable.Columns.Add("UPDATE_RULE");
			dataTable.Columns.Add("DELETE_RULE");
			dataTable.Columns.Add("FK_NAME");
			dataTable.Columns.Add("PK_NAME");
			dataTable.Columns.Add("DEFERRABILITY");

			DeveelDbCommand command = connection.CreateCommand("   SELECT * FROM INFORMATION_SCHEMA.imported_keys \n" +
			                                                   "    WHERE ( ? IS NULL OR \"PKTABLE_SCHEMA\" = ? ) \n" +
			                                                   "      AND \"PKTABLE_NAME\" = ? \n" +
			                                                   "ORDER BY \"FKTABLE_SCHEMA\", \"FKTABLE_NAME\", \"KEY_SEQ\"");

			command.Parameters.Add(schema);
			command.Parameters.Add(schema);
			command.Parameters.Add(table);

			command.Prepare();

			using (DeveelDbDataReader reader = command.ExecuteReader()) {
				if (reader.HasRows) {
					while (reader.Read()) {
						var row = dataTable.NewRow();
						row["PKTABLE_CATALOG"] = reader.GetString(0);
						row["PKTABLE_SCHEMA"] = reader.GetString(1);
						row["PKTABLE_NAME"] = reader.GetString(2);
						row["PKCOLUMN_NAME"] = reader.GetString(3);
						row["FKTABLE_CATALOG"] = reader.GetString(4);
						row["FKTABLE_SCHEMA"] = reader.GetString(5);
						row["FKTABLE_NAME"] = reader.GetString(6);
						row["FKCOLUMN_NAME"] = reader.GetString(7);
						row["KEY_SEQ"] = reader.GetInt32(8);
						row["UPDATE_RULE"] = reader.GetString(9);
						row["DELETE_RULE"] = reader.GetString(10);
						row["FK_NAME"] = reader.GetString(11);
						row["PK_NAME"] = reader.GetString(12);
						row["DEFERRABILITY"] = reader.GetInt32(13);
						dataTable.Rows.Add(row);
					}
				}
			}

			return dataTable;
		}

		public SysDataTable GetRestrictions() {
			object[][] restrictions = new object[][]
                {
					new object[] {"Schemata", "Schema", "", 0},
                    new object[] {"Tables", "Catalog", "", 0},
                    new object[] {"Tables", "Schema", "", 1},
                    new object[] {"Tables", "Table", "", 2},
                    new object[] {"Tables", "TableType", "", 3},
                    new object[] {"Columns", "Catalog", "", 0},
                    new object[] {"Columns", "Schema", "", 1},
                    new object[] {"Columns", "Table", "", 2},
                    new object[] {"Columns", "Column", "", 3},
                    new object[] {"PrimaryKeys", "Database", "", 0},
                    new object[] {"PrimaryKeys", "Schema", "", 1},
                    new object[] {"PrimaryKeys", "Table", "", 2},
                    new object[] {"ExportedKeys", "Catalog", "", 0},
                    new object[] {"ExportedKeys", "Schema", "", 1},
                    new object[] {"ExportedKeys", "Table", "", 2},
					new object[] {"ImportedKeys", "Catalog", "", 0},
					new object[] {"ImportedKeys", "Schema", "", 1}, 
					new object[] {"ImportedKeys", "Table", "", 2},
					new object[] {"ColumnPrivileges", "Catalog", "", 0},
					new object[] {"ColumnPrivileges", "Schema", "", 1},
					new object[] {"ColumnPrivileges", "Table", "", 2}, 
					new object[] {"ColumnPrivileges", "Column", "", 3},
					new object[] {"TablePrivileges", "Catalog", "", 0},
					new object[] {"TablePrivileges", "Schema", "", 1}, 
					new object[] {"TablePrivileges", "Table", "", 2},
					new object[] {"UserPrivileges", "UserName", "", 0}
                };

			SysDataTable dt = new SysDataTable("Restrictions");
			dt.Columns.Add(new DataColumn("CollectionName", typeof(string)));
			dt.Columns.Add(new DataColumn("RestrictionName", typeof(string)));
			dt.Columns.Add(new DataColumn("RestrictionDefault", typeof(string)));
			dt.Columns.Add(new DataColumn("RestrictionNumber", typeof(int)));

			FillTable(dt, restrictions);

			return dt;
		}

		private SysDataTable GetCollections() {
			object[][] collections = new object[][]
                {
                    new object[] {"MetaDataCollections", 0, 0},
                    new object[] {"DataSourceInformation", 0, 0},
                    new object[] {"DataTypes", 0, 0},
                    new object[] {"Restrictions", 0, 0},
                    new object[] {"ReservedWords", 0, 0},
                    new object[] {"Databases", 1, 1},
                    new object[] {"Tables", 4, 2},
                    new object[] {"Columns", 4, 4},
                    new object[] {"PrimaryKeys", 4, 3},
					new object[] {"ExportedKeys", 4, 3},
					new object[] {"ImportedKeys", 4, 3},
					new object[] { "UserPrivileges", 1, 0}
                };

			SysDataTable dt = new SysDataTable("MetaDataCollections");
			dt.Columns.Add("CollectionName", typeof(string));
			dt.Columns.Add("NumberOfRestrictions", typeof(int));
			dt.Columns.Add("NumberOfIdentifierParts", typeof(int));

			FillTable(dt, collections);

			return dt;
		}

		private SysDataTable GetDataSourceInformation() {
			SysDataTable dt = new SysDataTable("DataSourceInformation");
			dt.Columns.Add("CompositeIdentifierSeparatorPattern", typeof(string));
			dt.Columns.Add("DataSourceProductName", typeof(string));
			dt.Columns.Add("DataSourceProductVersion", typeof(string));
			dt.Columns.Add("DataSourceProductVersionNormalized", typeof(string));
			dt.Columns.Add("GroupByBehavior", typeof(GroupByBehavior));
			dt.Columns.Add("IdentifierPattern", typeof(string));
			dt.Columns.Add("IdentifierCase", typeof(IdentifierCase));
			dt.Columns.Add("OrderByColumnsInSelect", typeof(bool));
			dt.Columns.Add("ParameterMarkerFormat", typeof(string));
			dt.Columns.Add("ParameterMarkerPattern", typeof(string));
			dt.Columns.Add("ParameterNameMaxLength", typeof(int));
			dt.Columns.Add("ParameterNamePattern", typeof(string));
			dt.Columns.Add("QuotedIdentifierPattern", typeof(string));
			dt.Columns.Add("QuotedIdentifierCase", typeof(IdentifierCase));
			dt.Columns.Add("StatementSeparatorPattern", typeof(string));
			dt.Columns.Add("StringLiteralPattern", typeof(string));
			dt.Columns.Add("SupportedJoinOperators", typeof(SupportedJoinOperators));

			SysDataRow row = dt.NewRow();
			row["CompositeIdentifierSeparatorPattern"] = "\\.";
			row["DataSourceProductName"] = "DeveelDB";
			row["DataSourceProductVersion"] = connection.ServerVersion;
			row["DataSourceProductVersionNormalized"] = connection.ServerVersion;
			row["GroupByBehavior"] = GroupByBehavior.Unrelated;
			row["IdentifierPattern"] =
				@"(^\`\p{Lo}\p{Lu}\p{Ll}_@#][\p{Lo}\p{Lu}\p{Ll}\p{Nd}@$#_]*$)|(^\`[^\`\0]|\`\`+\`$)|(^\"" + [^\""\0]|\""\""+\""$)";
			row["IdentifierCase"] = IdentifierCase.Insensitive;
			row["OrderByColumnsInSelect"] = false;
			row["ParameterMarkerFormat"] = "{0}";
			row["ParameterMarkerPattern"] = "(@[A-Za-z0-9_$#]*)";
			row["ParameterNameMaxLength"] = 128;
			row["ParameterNamePattern"] = @"^[\p{Lo}\p{Lu}\p{Ll}\p{Lm}_@#][\p{Lo}\p{Lu}\p{Ll}\p{Lm}\p{Nd}\uff3f_@#\$]*(?=\s+|$)";
			row["QuotedIdentifierPattern"] = @"(([^\`]|\`\`)*)";
			row["QuotedIdentifierCase"] = IdentifierCase.Sensitive;
			row["StatementSeparatorPattern"] = ";";
			row["StringLiteralPattern"] = "'(([^']|'')*)'";
			row["SupportedJoinOperators"] = 15;
			dt.Rows.Add(row);

			return dt;
		}

		public SysDataTable GetDataTypes() {
			SysDataTable dataTable = new SysDataTable("DataTypes");

			dataTable.Columns.Add("TYPE_NAME", typeof(string));
			dataTable.Columns.Add("DATA_TYPE", typeof(int));
			dataTable.Columns.Add("PRECISION", typeof(int));
			dataTable.Columns.Add("LITERAL_PREFIX", typeof(string));
			dataTable.Columns.Add("LITERAL_SUFFIX", typeof(string));
			dataTable.Columns.Add("CREATE_PARAMS", typeof(string));
			dataTable.Columns.Add("NULLABLE", typeof(bool));
			dataTable.Columns.Add("CASE_SENSITIVE", typeof(bool));
			dataTable.Columns.Add("SEARCHABLE", typeof(bool));
			dataTable.Columns.Add("UNSIGNED_ATTRIBUTE", typeof(bool));
			dataTable.Columns.Add("FIXED_PREC_SCALE", typeof(bool));
			dataTable.Columns.Add("AUTO_INCREMENT", typeof(bool));
			dataTable.Columns.Add("LOCAL_TYPE_NAME");
			dataTable.Columns.Add("MINIMUM_SCALE", typeof(int));
			dataTable.Columns.Add("MAXIMUM_SCALE", typeof(int));
			dataTable.Columns.Add("SQL_DATA_TYPE", typeof(string));
			dataTable.Columns.Add("SQL_DATETIME_SUB", typeof(string));
			dataTable.Columns.Add("NUM_PREC_RADIX", typeof(int));

			DeveelDbCommand command = connection.CreateCommand("SELECT * FROM INFORMATION_SCHEMA.DATA_TYPES");

			using (DeveelDbDataReader reader = command.ExecuteReader()) {
				while (reader.Read()) {
					SysDataRow row = dataTable.NewRow();

					row["TYPE_NAME"] = reader.GetString(0);
					row["DATA_TYPE"] = reader.GetInt32(1);
					row["PRECISION"] = reader.GetInt32(2);
					row["LITERAL_PREFIX"] = reader.GetString(3);
					row["LITERAL_SUFFIX"] = reader.GetString(4);
					row["CREATE_PARAMS"] = reader.GetString(5);
					row["NULLABLE"] = reader.GetBoolean(6);
					row["CASE_SENSITIVE"] = reader.GetBoolean(7);
					row["SEARCHABLE"] = reader.GetBoolean(8);
					row["UNSIGNED_ATTRIBUTE"] = reader.GetBoolean(9);
					row["FIXED_PREC_SCALE"] = reader.GetBoolean(10);
					row["AUTO_INCREMENT"] = reader.GetBoolean(11);
					row["LOCAL_TYPE_NAME"] = reader.GetString(12);
					row["MINIMUM_SCALE"] = reader.GetInt32(13);
					row["MAXIMUM_SCALE"] = reader.GetInt32(14);
					row["SQL_DATA_TYPE"] = reader.GetString(15);
					row["SQL_DATETIME_SUB"] = reader.GetString(16);
					row["NUM_PREC_RADIX"] = reader.GetInt32(17);

					dataTable.Rows.Add(row);
				}
			}

			return dataTable;
		}

		public virtual SysDataTable GetUserPrivileges(string[] restrictions) {
			if (restrictions == null)
				throw new ArgumentNullException("restrictions");
			if (restrictions.Length < 1)
				throw new ArgumentException();

			var userName = restrictions[0];

			var dataTable = new SysDataTable("UserPrivileges");
			dataTable.Columns.Add("TABLE_CATALOG");
			dataTable.Columns.Add("GRANTEE");
			dataTable.Columns.Add("OBJECT_TYPE");
			dataTable.Columns.Add("OBJECT_NAME");
			dataTable.Columns.Add("PRIVS");
			dataTable.Columns.Add("IS_GRANTABLE", typeof(bool));
			dataTable.Columns.Add("GRANTER");

			DeveelDbCommand command = connection.CreateCommand("SELECT * FROM INFORMATION_SCHEMA.USER_PRIVILEGES WHERE (? IS NULL OR \"GRANTEE\" = ?)");
			command.Parameters.Add(userName);
			command.Parameters.Add(userName);

			command.Prepare();

			using (var reader = command.ExecuteReader()) {
				while (reader.Read()) {
					var row = dataTable.NewRow();
					row["TABLE_CATALOG"] = reader.GetString(0);
					row["GRANTEE"] = reader.GetString(1);
					row["OBJECT_TYPE"] = reader.GetString(2);
					row["OBJECT_NAME"] = reader.GetString(3);
					row["PRIVS"] = reader.GetString(4);
					row["IS_GRANTABLE"] = reader.GetBoolean(5);
					dataTable.Rows.Add(row);
				}
			}

			return dataTable;
		}

		public virtual SysDataTable GetSchema(string collection, String[] restrictions) {
			if (connection.State != ConnectionState.Open)
				throw new DataException("GetSchema can only be called on an open connection.");

			collection = collection.ToUpper(CultureInfo.InvariantCulture);

			SysDataTable dt = null;

			switch (collection) {
				// common collections
				case "METADATACOLLECTIONS":
					dt = GetCollections();
					break;
				case "DATASOURCEINFORMATION":
					dt = GetDataSourceInformation();
					break;
				case "DATATYPES":
					dt = GetDataTypes();
					break;
				case "RESTRICTIONS":
					dt = GetRestrictions();
					break;
				case "RESERVEDWORDS":
					//TODO: dt = GetReservedWords();
					break;
			}

			if (restrictions == null)
				restrictions = new string[2];
			if (connection != null &&
				connection.Settings.Schema != null &&
				connection.Settings.Schema.Length > 0 &&
				restrictions.Length > 1 &&
				restrictions[1] == null)
				restrictions[1] = connection.Settings.Schema;

			switch (collection) {
				case "SCHEMATA":
					dt = GetSchemata(restrictions);
					break;
				case "TABLES":
					dt = GetTables(restrictions);
					break;
				case "COLUMNS":
					dt = GetColumns(restrictions);
					break;
				case "TABLEPRIVILEGES":
					dt = GetTablePrivileges(restrictions);
					break;
				case "COLUMNPRIVILEGES":
					dt = GetColumnPrivileges(restrictions);
					break;
				case "PRIMARYKEYS":
					dt = GetPrimaryKeys(restrictions);
					break;
				case "EXPORTEDKEYS":
					dt = GetExportedKeys(restrictions);
					break;
				case "IMPORTEDKEYS":
					dt = GetImportedKeys(restrictions);
					break;
				case "USERPRIVILEGES":
					dt = GetUserPrivileges(restrictions);
					break;
			}


			if (dt == null)
				throw new DataException("Invalid collection name");

			return dt;
		}

		private static void FillTable(SysDataTable dt, object[][] data) {
			foreach (object[] dataItem in data) {
				SysDataRow row = dt.NewRow();
				for (int i = 0; i < dataItem.Length; i++)
					row[i] = dataItem[i];
				dt.Rows.Add(row);
			}
		}
	}
}