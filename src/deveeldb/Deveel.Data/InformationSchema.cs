using System;
using System.Data;

using Deveel.Data.Client;
using Deveel.Data.Security;
using Deveel.Diagnostics;

namespace Deveel.Data {
	public static class InformationSchema {
		/// <summary>
		///  Creates all the system views.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="logger"></param>
		internal static void CreateSystemViews(DatabaseConnection connection, Logger logger) {
			// Obtain the data interface.
			try {
				IDbConnection dbConn = connection.GetDbConnection();

				// Is the username/password in the database?
				IDbCommand stmt = dbConn.CreateCommand();

				// This view shows the grants that the user has (no join, only priv_bit).
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserSimpleGrant AS " +
					"  SELECT \"priv_bit\", \"object\", \"param\", \"grantee\", " +
					"         \"grant_option\", \"granter\" " +
					"    FROM SYSTEM.grant " +
					"   WHERE ( grantee = user() OR grantee = '@PUBLIC' )";
				stmt.ExecuteNonQuery();

				// This view shows the grants that the user is allowed to see
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserGrant AS " +
					"  SELECT \"description\", \"object\", \"param\", \"grantee\", " +
					"         \"grant_option\", \"granter\" " +
					"    FROM SYSTEM.grant, SYSTEM.priv_map " +
					"   WHERE ( grantee = user() OR grantee = '@PUBLIC' )" +
					"     AND grant.priv_bit = priv_map.priv_bit";
				stmt.ExecuteNonQuery();

				// A view that represents the list of schema this user is allowed to view
				// the contents of.
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserSchemaInfo AS " +
					"  SELECT * FROM SYSTEM.schema_info " +
					"   WHERE \"name\" IN ( " +
					"     SELECT \"param\" " +
					"       FROM INFORMATION_SCHEMA.ThisUserGrant " +
					"      WHERE \"object\" = 65 " +
					"        AND \"description\" = 'LIST' )";
				stmt.ExecuteNonQuery();

				// A view that exposes the table_columns table but only for the tables
				// this user has Read access to.
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserTableColumns AS " +
					"  SELECT * FROM SYSTEM.table_columns " +
					"   WHERE \"schema\" IN ( " +
					"     SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )";
				stmt.ExecuteNonQuery();

				// A view that exposes the 'table_info' table but only for the tables
				// this user has Read access to.
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserTableInfo AS " +
					"  SELECT * FROM SYSTEM.table_info " +
					"   WHERE \"schema\" IN ( " +
					"     SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.TABLES AS " +
					"  SELECT NULL AS \"TABLE_CATALOG\", \n" +
					"         \"schema\" AS \"TABLE_SCHEMA\", \n" +
					"         \"name\" AS \"TABLE_NAME\", \n" +
					"         \"type\" AS \"TABLE_TYPE\", \n" +
					"         \"other\" AS \"REMARKS\", \n" +
					"         NULL AS \"TYPE_CATALOG\", \n" +
					"         NULL AS \"TYPE_SCHEMA\", \n" +
					"         NULL AS \"TYPE_NAME\", \n" +
					"         NULL AS \"SELF_REFERENCING_COL_NAME\", \n" +
					"         NULL AS \"REF_GENERATION\" \n" +
					"    FROM INFORMATION_SCHEMA.ThisUserTableInfo \n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.SCHEMATA AS " +
					"  SELECT \"name\" AS \"TABLE_SCHEMA\", \n" +
					"         NULL AS \"TABLE_CATALOG\" \n" +
					"    FROM INFORMATION_SCHEMA.ThisUserSchemaInfo\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.CATALOGS AS " +
					"  SELECT NULL AS \"TABLE_CATALOG\" \n" +
					"    FROM SYSTEM.schema_info\n" + // Hacky, this will generate a 0 row
					"   WHERE FALSE\n"; // table.
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.COLUMNS AS " +
					"  SELECT NULL AS \"TABLE_CATALOG\",\n" +
					"         \"schema\" AS \"TABLE_SCHEMA\",\n" +
					"         \"table\" AS \"TABLE_NAME\",\n" +
					"         \"column\" AS \"COLUMN_NAME\",\n" +
					"         \"sql_type\" AS \"DATA_TYPE\",\n" +
					"         \"type_desc\" AS \"TYPE_NAME\",\n" +
					"         IF(\"size\" = -1, 1024, \"size\") AS \"COLUMN_SIZE\",\n" +
					"         NULL AS \"BUFFER_LENGTH\",\n" +
					"         \"scale\" AS \"DECIMAL_DIGITS\",\n" +
					"         IF(\"sql_type\" = -7, 2, 10) AS \"NUM_PREC_RADIX\",\n" +
					"         IF(\"not_null\", 0, 1) AS \"NULLABLE\",\n" +
					"         '' AS \"REMARKS\",\n" +
					"         \"default\" AS \"COLUMN_DEFAULT\",\n" +
					"         NULL AS \"SQL_DATA_TYPE\",\n" +
					"         NULL AS \"SQL_DATETIME_SUB\",\n" +
					"         IF(\"size\" = -1, 1024, \"size\") AS \"CHAR_OCTET_LENGTH\",\n" +
					"         \"seq_no\" + 1 AS \"ORDINAL_POSITION\",\n" +
					"         IF(\"not_null\", 'NO', 'YES') AS \"IS_NULLABLE\"\n" +
					"    FROM INFORMATION_SCHEMA.ThisUserTableColumns\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.COLUMN_PRIVILEGES AS " +
					"  SELECT \"TABLE_CATALOG\",\n" +
					"         \"TABLE_SCHEMA\",\n" +
					"         \"TABLE_NAME\",\n" +
					"         \"COLUMN_NAME\",\n" +
					"         IF(\"ThisUserGrant.granter\" = '@SYSTEM', \n" +
					"                        NULL, \"ThisUserGrant.granter\") AS \"GRANTOR\",\n" +
					"         IF(\"ThisUserGrant.grantee\" = '@PUBLIC', \n" +
					"                    'public', \"ThisUserGrant.grantee\") AS \"GRANTEE\",\n" +
					"         \"ThisUserGrant.description\" AS \"PRIVILEGE\",\n" +
					"         IF(\"grant_option\" = 'true', 'YES', 'NO') AS \"IS_GRANTABLE\" \n" +
					"    FROM INFORMATION_SCHEMA.COLUMNS, INFORMATION_SCHEMA.ThisUserGrant \n" +
					"   WHERE CONCAT(COLUMNS.TABLE_SCHEMA, '.', COLUMNS.TABLE_NAME) = \n" +
					"         ThisUserGrant.param \n" +
					"     AND INFORMATION_SCHEMA.ThisUserGrant.object = 1 \n" +
					"     AND INFORMATION_SCHEMA.ThisUserGrant.description IS NOT NULL \n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.TABLE_PRIVILEGES AS " +
					"  SELECT \"TABLE_CATALOG\",\n" +
					"         \"TABLE_SCHEMA\",\n" +
					"         \"TABLE_NAME\",\n" +
					"         IF(\"ThisUserGrant.granter\" = '@SYSTEM', \n" +
					"                        NULL, \"ThisUserGrant.granter\") AS \"GRANTOR\",\n" +
					"         IF(\"ThisUserGrant.grantee\" = '@PUBLIC', \n" +
					"                    'public', \"ThisUserGrant.grantee\") AS \"GRANTEE\",\n" +
					"         \"ThisUserGrant.description\" AS \"PRIVILEGE\",\n" +
					"         IF(\"grant_option\" = 'true', 'YES', 'NO') AS \"IS_GRANTABLE\" \n" +
					"    FROM INFORMATION_SCHEMA.TABLES, INFORMATION_SCHEMA.ThisUserGrant \n" +
					"   WHERE CONCAT(TABLES.TABLE_SCHEMA, '.', TABLES.TABLE_NAME) = \n" +
					"         ThisUserGrant.param \n" +
					"     AND INFORMATION_SCHEMA.ThisUserGrant.object = 1 \n" +
					"     AND INFORMATION_SCHEMA.ThisUserGrant.description IS NOT NULL \n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.PrimaryKeys AS " +
					"  SELECT NULL \"TABLE_CATALOG\",\n" +
					"         \"schema\" \"TABLE_SCHEMA\",\n" +
					"         \"table\" \"TABLE_NAME\",\n" +
					"         \"column\" \"COLUMN_NAME\",\n" +
					"         \"SYSTEM.primary_columns.seq_no\" \"KEY_SEQ\",\n" +
					"         \"name\" \"PK_NAME\"\n" +
					"    FROM SYSTEM.pkey_info, SYSTEM.primary_columns\n" +
					"   WHERE pkey_info.id = primary_columns.pk_id\n" +
					"     AND \"schema\" IN\n" +
					"            ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.ImportedKeys AS " +
					"  SELECT NULL \"PKTABLE_CATALOG\",\n" +
					"         \"fkey_info.ref_schema\" \"PKTABLE_SCHEMA\",\n" +
					"         \"fkey_info.ref_table\" \"PKTABLE_NAME\",\n" +
					"         \"foreign_columns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
					"         NULL \"FKTABLE_CATALOG\",\n" +
					"         \"fkey_info.schema\" \"FKTABLE_SCHEMA\",\n" +
					"         \"fkey_info.table\" \"FKTABLE_NAME\",\n" +
					"         \"foreign_columns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
					"         \"foreign_columns.seq_no\" \"KEY_SEQ\",\n" +
					"         I_FRULE_CONVERT(\"fkey_info.update_rule\") \"UPDATE_RULE\",\n" +
					"         I_FRULE_CONVERT(\"fkey_info.delete_rule\") \"DELETE_RULE\",\n" +
					"         \"fkey_info.name\" \"FK_NAME\",\n" +
					"         NULL \"PK_NAME\",\n" +
					"         \"fkey_info.deferred\" \"DEFERRABILITY\"\n" +
					"    FROM SYSTEM.fkey_info, SYSTEM.foreign_columns\n" +
					"   WHERE fkey_info.id = foreign_columns.fk_id\n" +
					"     AND \"fkey_info.schema\" IN\n" +
					"              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.ExportedKeys AS " +
					"  SELECT NULL \"PKTABLE_CAT\",\n" +
					"         \"fkey_info.ref_schema\" \"PKTABLE_SCHEMA\",\n" +
					"         \"fkey_info.ref_table\" \"PKTABLE_NAME\",\n" +
					"         \"foreign_columns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
					"         NULL \"FKTABLE_CATALOG\",\n" +
					"         \"fkey_info.schema\" \"FKTABLE_SCHEMA\",\n" +
					"         \"fkey_info.table\" \"FKTABLE_NAME\",\n" +
					"         \"foreign_columns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
					"         \"foreign_columns.seq_no\" \"KEY_SEQ\",\n" +
					"         I_FRULE_CONVERT(\"fkey_info.update_rule\") \"UPDATE_RULE\",\n" +
					"         I_FRULE_CONVERT(\"fkey_info.delete_rule\") \"DELETE_RULE\",\n" +
					"         \"fkey_info.name\" \"FK_NAME\",\n" +
					"         NULL \"PK_NAME\",\n" +
					"         \"fkey_info.deferred\" \"DEFERRABILITY\"\n" +
					"    FROM SYSTEM.fkey_info, SYSTEM.foreign_columns\n" +
					"   WHERE fkey_info.id = foreign_columns.fk_id\n" +
					"     AND \"fkey_info.schema\" IN\n" +
					"              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.CrossReference AS " +
					"  SELECT NULL \"PKTABLE_CAT\",\n" +
					"         \"fkey_info.ref_schema\" \"PKTABLE_SCHEMA\",\n" +
					"         \"fkey_info.ref_table\" \"PKTABLE_NAME\",\n" +
					"         \"foreign_columns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
					"         NULL \"FKTABLE_CAT\",\n" +
					"         \"fkey_info.schema\" \"FKTABLE_SCHEMA\",\n" +
					"         \"fkey_info.table\" \"FKTABLE_NAME\",\n" +
					"         \"foreign_columns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
					"         \"foreign_columns.seq_no\" \"KEY_SEQ\",\n" +
					"         I_FRULE_CONVERT(\"fkey_info.update_rule\") \"UPDATE_RULE\",\n" +
					"         I_FRULE_CONVERT(\"fkey_info.delete_rule\") \"DELETE_RULE\",\n" +
					"         \"fkey_info.name\" \"FK_NAME\",\n" +
					"         NULL \"PK_NAME\",\n" +
					"         \"fkey_info.deferred\" \"DEFERRABILITY\"\n" +
					"    FROM SYSTEM.fkey_info, SYSTEM.foreign_columns\n" +
					"   WHERE fkey_info.id = foreign_columns.fk_id\n" +
					"     AND \"fkey_info.schema\" IN\n" +
					"              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				// export all the built-in data types...
				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.DATA_TYPES AS " +
					"  SELECT * FROM SYSTEM.sql_types\n";
				stmt.ExecuteNonQuery();

				//TODO: export the variables too...
			} catch (DataException e) {
				if (e is DbDataException) {
					DbDataException dbDataException = (DbDataException)e;
					logger.Error(connection, dbDataException.ServerErrorStackTrace);
				}
				logger.Error(connection, e);
				throw new Exception("SQL Error: " + e.Message);
			}
		}

		internal static void SetViewsGrants(GrantManager manager, string granter) {
			// Set public grants for the system views.
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserGrant",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserSimpleGrant",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserSchemaInfo",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserTableColumns",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserTableInfo",
						  GrantManager.PublicUsernameStr, false, granter);

			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.TABLES", GrantManager.PublicUsernameStr,
						  false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.SCHEMATA", GrantManager.PublicUsernameStr,
						  false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.CATALOGS", GrantManager.PublicUsernameStr,
						  false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.COLUMNS", GrantManager.PublicUsernameStr,
						  false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.COLUMN_PRIVILEGES",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.TABLE_PRIVILEGES",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.PrimaryKeys",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ImportedKeys",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ExportedKeys",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.CrossReference",
						  GrantManager.PublicUsernameStr, false, granter);
		}

		/// <summary>
		/// The name of the schema that contains helper tables.
		/// </summary>
		public const string Name = "INFORMATION_SCHEMA";
	}
}
