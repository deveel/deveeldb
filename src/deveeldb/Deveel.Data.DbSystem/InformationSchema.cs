using System;
using System.Data;

using Deveel.Data.Client;
using Deveel.Data.Security;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	public static class InformationSchema {
		/// <summary>
		/// The name of the schema that contains helper tables.
		/// </summary>
		public const string Name = "INFORMATION_SCHEMA";

		public static readonly TableName Catalogs = new TableName(Name, "catalogs");

		public static readonly TableName Tables = new TableName(Name, "tables");

		public static readonly TableName TablePrivileges = new TableName(Name, "table_privileges");

		public static readonly TableName Schemata = new TableName(Name, "schemata");

		public static readonly TableName Columns = new TableName(Name, "columns");

		public static readonly TableName ColumnPrivileges = new TableName(Name, "column_privileges");

		public static readonly TableName PrimaryKeys = new TableName(Name, "primary_keys");

		public static readonly TableName ImportedKeys = new TableName(Name, "imported_keys");

		public static readonly TableName ExportedKeys = new TableName(Name, "exported_keys");

		public static readonly TableName DataTypes = new TableName(Name, "data_types");

		public static readonly TableName CrossReference = new TableName(Name, "cross_reference");

		public static readonly TableName UserPrivileges = new TableName(Name, "user_privileges");

		/// <summary>
		///  Creates all the system views.
		/// </summary>
		/// <param name="connection"></param>
		internal static void CreateSystemViews(IDatabaseConnection connection) {
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
					"    FROM " + SystemSchema.Grant +
					"   WHERE ( grantee = user() OR grantee = '@PUBLIC' )";
				stmt.ExecuteNonQuery();

				// This view shows the grants that the user is allowed to see
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserGrant AS " +
					"  SELECT \"description\", \"object\", \"param\", \"grantee\", " +
					"         \"grant_option\", \"granter\" " +
					"    FROM "+ SystemSchema.Grant+", " + SystemSchema.Privileges +
					"   WHERE ( grantee = user() OR grantee = '@PUBLIC' )" +
					"     AND grant.priv_bit = priv_map.priv_bit";
				stmt.ExecuteNonQuery();

				// A view that represents the list of schema this user is allowed to view
				// the contents of.
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserSchemaInfo AS " +
					"  SELECT * FROM  " + SystemSchema.SchemaInfoTable +
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
					"  SELECT * FROM "+ SystemSchema.TableColumns +
					"   WHERE \"schema\" IN ( " +
					"     SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )";
				stmt.ExecuteNonQuery();

				// A view that exposes the 'table_info' table but only for the tables
				// this user has Read access to.
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserTableInfo AS " +
					"  SELECT * FROM " + SystemSchema.TableInfo +
					"   WHERE \"schema\" IN ( " +
					"     SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW "+ Tables +" AS " +
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
					"  CREATE VIEW "+ Schemata + " AS " +
					"  SELECT \"name\" AS \"TABLE_SCHEMA\", \n" +
					"         NULL AS \"TABLE_CATALOG\" \n" +
					"    FROM INFORMATION_SCHEMA.ThisUserSchemaInfo\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW "+Catalogs+" AS " +
					"  SELECT NULL AS \"TABLE_CATALOG\" \n" +
					"    FROM "+ SystemSchema.SchemaInfoTable+"\n" + // Hacky, this will generate a 0 row
					"   WHERE FALSE\n"; // table.
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW "+ Columns +" AS " +
					"  SELECT NULL AS \"TABLE_CATALOG\",\n" +
					"         \"schema\" AS \"TABLE_SCHEMA\",\n" +
					"         \"table\" AS \"TABLE_NAME\",\n" +
					"         \"column\" AS \"COLUMN_NAME\",\n" +
					"         \"sql_type\" AS \"DATA_TYPE\",\n" +
					"         \"type_desc\" AS \"TYPE_NAME\",\n" +
					"         IIF(\"size\" = -1, 1024, \"size\") AS \"COLUMN_SIZE\",\n" +
					"         NULL AS \"BUFFER_LENGTH\",\n" +
					"         \"scale\" AS \"DECIMAL_DIGITS\",\n" +
					"         IIF(\"sql_type\" = -7, 2, 10) AS \"NUM_PREC_RADIX\",\n" +
					"         IIF(\"not_null\", 0, 1) AS \"NULLABLE\",\n" +
					"         '' AS \"REMARKS\",\n" +
					"         \"default\" AS \"COLUMN_DEFAULT\",\n" +
					"         NULL AS \"SQL_DATA_TYPE\",\n" +
					"         NULL AS \"SQL_DATETIME_SUB\",\n" +
					"         IIF(\"size\" = -1, 1024, \"size\") AS \"CHAR_OCTET_LENGTH\",\n" +
					"         \"seq_no\" + 1 AS \"ORDINAL_POSITION\",\n" +
					"         IIF(\"not_null\", 'NO', 'YES') AS \"IS_NULLABLE\"\n" +
					"    FROM INFORMATION_SCHEMA.ThisUserTableColumns\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW "+ ColumnPrivileges +" AS " +
					"  SELECT \"TABLE_CATALOG\",\n" +
					"         \"TABLE_SCHEMA\",\n" +
					"         \"TABLE_NAME\",\n" +
					"         \"COLUMN_NAME\",\n" +
					"         IIF(\"ThisUserGrant.granter\" = '@SYSTEM', \n" +
					"                        NULL, \"ThisUserGrant.granter\") AS \"GRANTOR\",\n" +
					"         IIF(\"ThisUserGrant.grantee\" = '@PUBLIC', \n" +
					"                    'public', \"ThisUserGrant.grantee\") AS \"GRANTEE\",\n" +
					"         \"ThisUserGrant.description\" AS \"PRIVILEGE\",\n" +
					"         IIF(\"grant_option\" = 'true', 'YES', 'NO') AS \"IS_GRANTABLE\" \n" +
					"    FROM "+Columns+", INFORMATION_SCHEMA.ThisUserGrant \n" +
					"   WHERE CONCAT(columns.TABLE_SCHEMA, '.', columns.TABLE_NAME) = \n" +
					"         ThisUserGrant.param \n" +
					"     AND INFORMATION_SCHEMA.ThisUserGrant.object = 1 \n" +
					"     AND INFORMATION_SCHEMA.ThisUserGrant.description IS NOT NULL \n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW "+ TablePrivileges+" AS " +
					"  SELECT \"TABLE_CATALOG\",\n" +
					"         \"TABLE_SCHEMA\",\n" +
					"         \"TABLE_NAME\",\n" +
					"         IIF(\"ThisUserGrant.granter\" = '@SYSTEM', \n" +
					"                        NULL, \"ThisUserGrant.granter\") AS \"GRANTOR\",\n" +
					"         IIF(\"ThisUserGrant.grantee\" = '@PUBLIC', \n" +
					"                    'public', \"ThisUserGrant.grantee\") AS \"GRANTEE\",\n" +
					"         \"ThisUserGrant.description\" AS \"PRIVILEGE\",\n" +
					"         IIF(\"grant_option\" = 'true', 'YES', 'NO') AS \"IS_GRANTABLE\" \n" +
					"    FROM "+ Tables+", INFORMATION_SCHEMA.ThisUserGrant \n" +
					"   WHERE CONCAT(tables.TABLE_SCHEMA, '.', tables.TABLE_NAME) = \n" +
					"         ThisUserGrant.param \n" +
					"     AND INFORMATION_SCHEMA.ThisUserGrant.object = 1 \n" +
					"     AND INFORMATION_SCHEMA.ThisUserGrant.description IS NOT NULL \n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW "+ PrimaryKeys+" AS " +
					"  SELECT NULL \"TABLE_CATALOG\",\n" +
					"         \"schema\" \"TABLE_SCHEMA\",\n" +
					"         \"table\" \"TABLE_NAME\",\n" +
					"         \"column\" \"COLUMN_NAME\",\n" +
					"         \"SYSTEM.primary_columns.seq_no\" \"KEY_SEQ\",\n" +
					"         \"name\" \"PK_NAME\"\n" +
					"    FROM "+ SystemSchema.PrimaryInfoTable+", "+ SystemSchema.PrimaryColsTable+"\n" +
					"   WHERE pkey_info.id = primary_columns.pk_id\n" +
					"     AND \"schema\" IN\n" +
					"            ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW "+ ImportedKeys+" AS " +
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
					"    FROM "+ SystemSchema.ForeignInfoTable+", "+ SystemSchema.ForeignColsTable+"\n" +
					"   WHERE fkey_info.id = foreign_columns.fk_id\n" +
					"     AND \"fkey_info.schema\" IN\n" +
					"              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW "+ ExportedKeys+" AS " +
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
					"    FROM "+ SystemSchema.ForeignInfoTable+", "+ SystemSchema.ForeignColsTable+"\n" +
					"   WHERE fkey_info.id = foreign_columns.fk_id\n" +
					"     AND \"fkey_info.schema\" IN\n" +
					"              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW "+ CrossReference+" AS " +
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
					"    FROM "+ SystemSchema.ForeignInfoTable+", "+ SystemSchema.ForeignColsTable+"\n" +
					"   WHERE fkey_info.id = foreign_columns.fk_id\n" +
					"     AND \"fkey_info.schema\" IN\n" +
					"              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				// export all the built-in data types...
				stmt.CommandText =
					"  CREATE VIEW " + DataTypes + " AS " +
					"  SELECT * FROM "+SystemSchema.SqlTypes+"\n";
				stmt.ExecuteNonQuery();

				//TODO: export the variables too...

				// TODO: Add functions to list
				stmt.CommandText =
					"  CREATE VIEW " + UserPrivileges + " AS \n" +
					"  SELECT NULL \"TABLE_CAT\", \n" +
					"         \"grant.grantee\",\n" +
					"         \"grant.object\" \"OBJECT_TYPE\"," +
					"         \"grant.param\" \"OBJECT_NAME\"," +
					"         I_PRIVILEGE_STRING(\"grant.priv_bit\") \"PRIVS\"," +
					"         \"grant.grant_option\" \"IS_GRANTABLE\",\n" +
					"         \"grant.granter\"\n" +
					"  FROM " + SystemSchema.Grant + "\n";
				stmt.ExecuteNonQuery();

			} catch (DataException e) {
				if (e is DbDataException) {
					DbDataException dbDataException = (DbDataException)e;
					connection.Database.Context.Logger.Error(connection, dbDataException.ServerErrorStackTrace);
				}
				connection.Database.Context.Logger.Error(connection, e);
				throw new Exception("SQL Error: " + e.Message);
			}
		}

		internal static void SetViewsGrants(GrantManager manager, string granter) {
			// Set public grants for the system views.
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserGrant",
						  User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserSimpleGrant",
						  User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserSchemaInfo",
						  User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserTableColumns",
						  User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserTableInfo",
						  User.PublicName, false, granter);

			manager.Grant(Privileges.TableRead, GrantObject.Table, Tables.ToString(), User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, Schemata.ToString(), User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, Catalogs.ToString(), User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, Columns.ToString(), User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, ColumnPrivileges.ToString(), User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, TablePrivileges.ToString(), User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, PrimaryKeys.ToString(), User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, ImportedKeys.ToString(), User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, ExportedKeys.ToString(), User.PublicName, false, granter);
			manager.Grant(Privileges.TableRead, GrantObject.Table, CrossReference.ToString(), User.PublicName, false, granter);
		}
	}
}
