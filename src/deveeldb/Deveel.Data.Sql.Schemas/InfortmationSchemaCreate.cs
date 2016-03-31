using System;

using Deveel.Data.Security;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Schemas {
	class InfortmationSchemaCreate : ISystemCreateCallback {
		private IQuery query;

		public InfortmationSchemaCreate(IQuery query) {
			this.query = query;
		}

		public void Activate(SystemCreatePhase phase) {
			if (phase == SystemCreatePhase.DatabaseCreate) {
				CreateViews();
			}
		}

		private void CreateViews() {
			CreateViews(query);
		}

		public static void CreateViews(IQuery query) {
			// This view shows the grants that the user has (no join, only priv_bit).
			query.ExecuteQuery("CREATE VIEW " +  InformationSchema.ThisUserSimpleGrantViewName + " AS " +
								 "  SELECT \"priv_bit\", \"object\", \"name\", \"grantee\", " +
								 "         \"grant_option\", \"granter\" " +
								 "    FROM " + SystemSchema.GrantsTableName +
								 "   WHERE ( grantee = user() OR grantee = '@PUBLIC' )");

			// This view shows the grants that the user is allowed to see
			query.ExecuteQuery("CREATE VIEW " + InformationSchema.ThisUserGrantViewName + " AS " +
								 "  SELECT \"description\", \"object\", \"name\", \"grantee\", " +
								 "         \"grant_option\", \"granter\" " +
								 "    FROM " + SystemSchema.GrantsTableName + ", " + SystemSchema.PrivilegesTableName +
								 "   WHERE ( grantee = user() OR grantee = '@PUBLIC' )" +
								 "     AND " + SystemSchema.GrantsTableName + ".priv_bit = " +
								 SystemSchema.PrivilegesTableName + ".priv_bit");

			// A view that represents the list of schema this user is allowed to view
			// the contents of.
			query.ExecuteQuery("CREATE VIEW " + InformationSchema.ThisUserSchemaInfoViewName + " AS " +
								 "  SELECT * FROM  " + SystemSchema.SchemaInfoTableName +
								 "   WHERE \"name\" IN ( " +
								 "     SELECT \"name\" " +
								 "       FROM " + InformationSchema.ThisUserGrantViewName + " " +
								 "      WHERE \"object\" = " + ((int)DbObjectType.Schema) +
								 "        AND \"description\" = '" + Privileges.List + "' )");

			// A view that exposes the table_columns table but only for the tables
			// this user has read access to.
			query.ExecuteQuery("CREATE VIEW " + InformationSchema.ThisUserTableColumnsViewName + " AS " +
								 "  SELECT * FROM " + SystemSchema.TableColumnsTableName +
								 "   WHERE \"schema\" IN ( " +
								 "     SELECT \"name\" FROM " + InformationSchema.ThisUserSchemaInfoViewName + ")");

			// A view that exposes the 'table_info' table but only for the tables
			// this user has read access to.
			query.ExecuteQuery("CREATE VIEW " + InformationSchema.ThisUserTableInfoViewName + " AS " +
								 "  SELECT * FROM " + SystemSchema.TableInfoTableName +
								 "   WHERE \"schema\" IN ( " +
								 "     SELECT \"name\" FROM " + InformationSchema.ThisUserSchemaInfoViewName + ")");

			query.ExecuteQuery("  CREATE VIEW " + InformationSchema.Tables + " AS " +
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
								 "    FROM " + InformationSchema.ThisUserTableInfoViewName + "\n");

			query.ExecuteQuery("  CREATE VIEW " + InformationSchema.Schemata + " AS " +
								 "  SELECT \"name\" AS \"TABLE_SCHEMA\", \n" +
								 "         NULL AS \"TABLE_CATALOG\" \n" +
								 "    FROM " + InformationSchema.ThisUserSchemaInfoViewName + "\n");

			query.ExecuteQuery("  CREATE VIEW " + InformationSchema.Catalogs + " AS " +
								 "  SELECT NULL AS \"TABLE_CATALOG\" \n" +
								 "    FROM " + SystemSchema.SchemaInfoTableName + "\n" + // Hacky, this will generate a 0 row
								 "   WHERE FALSE\n");

			query.ExecuteQuery("  CREATE VIEW " + InformationSchema.Columns + " AS " +
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
								 "    FROM " + InformationSchema.ThisUserTableColumnsViewName + "\n");

			query.ExecuteQuery("  CREATE VIEW " + InformationSchema.ColumnPrivileges + " AS " +
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
								 "    FROM " + InformationSchema.Columns + ", INFORMATION_SCHEMA.ThisUserGrant \n" +
								 "   WHERE CONCAT(columns.TABLE_SCHEMA, '.', columns.TABLE_NAME) = \n" +
								 "         ThisUserGrant.name \n" +
								 "     AND " + InformationSchema.ThisUserGrantViewName + ".object = 1 \n" +
								 "     AND " + InformationSchema.ThisUserGrantViewName + ".description IS NOT NULL \n");

			query.ExecuteQuery("  CREATE VIEW " + InformationSchema.TablePrivileges + " AS " +
								 "  SELECT \"TABLE_CATALOG\",\n" +
								 "         \"TABLE_SCHEMA\",\n" +
								 "         \"TABLE_NAME\",\n" +
								 "         IIF(\"ThisUserGrant.granter\" = '@SYSTEM', \n" +
								 "                        NULL, \"ThisUserGrant.granter\") AS \"GRANTOR\",\n" +
								 "         IIF(\"ThisUserGrant.grantee\" = '@PUBLIC', \n" +
								 "                    'public', \"ThisUserGrant.grantee\") AS \"GRANTEE\",\n" +
								 "         \"ThisUserGrant.description\" AS \"PRIVILEGE\",\n" +
								 "         IIF(\"grant_option\" = 'true', 'YES', 'NO') AS \"IS_GRANTABLE\" \n" +
								 "    FROM " + InformationSchema.Tables + ", " + InformationSchema.ThisUserGrantViewName + " \n" +
								 "   WHERE CONCAT(tables.TABLE_SCHEMA, '.', tables.TABLE_NAME) = \n" +
								 "         ThisUserGrant.name \n" +
								 "     AND " + InformationSchema.ThisUserGrantViewName + ".object = 1 \n" +
								 "     AND " + InformationSchema.ThisUserGrantViewName + ".description IS NOT NULL \n");

			query.ExecuteQuery("  CREATE VIEW " + InformationSchema.PrimaryKeys + " AS " +
								 "  SELECT NULL \"TABLE_CATALOG\",\n" +
								 "         \"schema\" \"TABLE_SCHEMA\",\n" +
								 "         \"table\" \"TABLE_NAME\",\n" +
								 "         \"column\" \"COLUMN_NAME\",\n" +
								 "         \"SYSTEM.pkey_cols.seq_no\" \"KEY_SEQ\",\n" +
								 "         \"name\" \"PK_NAME\"\n" +
								 "    FROM " + SystemSchema.PrimaryKeyInfoTableName + ", " + SystemSchema.PrimaryKeyColumnsTableName + "\n" +
								 "   WHERE pkey_info.id = pkey_cols.pk_id\n" +
								 "     AND \"schema\" IN\n" +
								 "            ( SELECT \"name\" FROM " + InformationSchema.ThisUserSchemaInfoViewName + " )\n");

			query.ExecuteQuery("  CREATE VIEW " + InformationSchema.ImportedKeys + " AS " +
								 "  SELECT NULL \"PKTABLE_CATALOG\",\n" +
								 "         \"fkey_info.ref_schema\" \"PKTABLE_SCHEMA\",\n" +
								 "         \"fkey_info.ref_table\" \"PKTABLE_NAME\",\n" +
								 "         \"fkey_cols.pcolumn\" \"PKCOLUMN_NAME\",\n" +
								 "         NULL \"FKTABLE_CATALOG\",\n" +
								 "         \"fkey_info.schema\" \"FKTABLE_SCHEMA\",\n" +
								 "         \"fkey_info.table\" \"FKTABLE_NAME\",\n" +
								 "         \"fkey_cols.fcolumn\" \"FKCOLUMN_NAME\",\n" +
								 "         \"fkey_cols.seq_no\" \"KEY_SEQ\",\n" +
								 "         I_FRULE_CONVERT(\"fkey_info.update_rule\") \"UPDATE_RULE\",\n" +
								 "         I_FRULE_CONVERT(\"fkey_info.delete_rule\") \"DELETE_RULE\",\n" +
								 "         \"fkey_info.name\" \"FK_NAME\",\n" +
								 "         NULL \"PK_NAME\",\n" +
								 "         \"fkey_info.deferred\" \"DEFERRABILITY\"\n" +
								 "    FROM " + SystemSchema.ForeignKeyInfoTableName + ", " + SystemSchema.ForeignKeyColumnsTableName + "\n" +
								 "   WHERE fkey_info.id = fkey_cols.fk_id\n" +
								 "     AND \"fkey_info.schema\" IN\n" +
								 "              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n");

			query.ExecuteQuery("  CREATE VIEW " + InformationSchema.ExportedKeys + " AS " +
								 "  SELECT NULL \"PKTABLE_CAT\",\n" +
								 "         \"fkey_info.ref_schema\" \"PKTABLE_SCHEMA\",\n" +
								 "         \"fkey_info.ref_table\" \"PKTABLE_NAME\",\n" +
								 "         \"fkey_cols.pcolumn\" \"PKCOLUMN_NAME\",\n" +
								 "         NULL \"FKTABLE_CATALOG\",\n" +
								 "         \"fkey_info.schema\" \"FKTABLE_SCHEMA\",\n" +
								 "         \"fkey_info.table\" \"FKTABLE_NAME\",\n" +
								 "         \"fkey_cols.fcolumn\" \"FKCOLUMN_NAME\",\n" +
								 "         \"fkey_cols.seq_no\" \"KEY_SEQ\",\n" +
								 "         I_FRULE_CONVERT(\"fkey_info.update_rule\") \"UPDATE_RULE\",\n" +
								 "         I_FRULE_CONVERT(\"fkey_info.delete_rule\") \"DELETE_RULE\",\n" +
								 "         \"fkey_info.name\" \"FK_NAME\",\n" +
								 "         NULL \"PK_NAME\",\n" +
								 "         \"fkey_info.deferred\" \"DEFERRABILITY\"\n" +
								 "    FROM " + SystemSchema.ForeignKeyInfoTableName + ", " + SystemSchema.ForeignKeyColumnsTableName + "\n" +
								 "   WHERE fkey_info.id = fkey_cols.fk_id\n" +
								 "     AND \"fkey_info.schema\" IN\n" +
								 "              ( SELECT \"name\" FROM " + InformationSchema.ThisUserSchemaInfoViewName + " )\n");

			query.ExecuteQuery("  CREATE VIEW " + InformationSchema.CrossReference + " AS " +
								 "  SELECT NULL \"PKTABLE_CAT\",\n" +
								 "         \"fkey_info.ref_schema\" \"PKTABLE_SCHEMA\",\n" +
								 "         \"fkey_info.ref_table\" \"PKTABLE_NAME\",\n" +
								 "         \"fkey_cols.pcolumn\" \"PKCOLUMN_NAME\",\n" +
								 "         NULL \"FKTABLE_CAT\",\n" +
								 "         \"fkey_info.schema\" \"FKTABLE_SCHEMA\",\n" +
								 "         \"fkey_info.table\" \"FKTABLE_NAME\",\n" +
								 "         \"fkey_cols.fcolumn\" \"FKCOLUMN_NAME\",\n" +
								 "         \"fkey_cols.seq_no\" \"KEY_SEQ\",\n" +
								 "         I_FRULE_CONVERT(\"fkey_info.update_rule\") \"UPDATE_RULE\",\n" +
								 "         I_FRULE_CONVERT(\"fkey_info.delete_rule\") \"DELETE_RULE\",\n" +
								 "         \"fkey_info.name\" \"FK_NAME\",\n" +
								 "         NULL \"PK_NAME\",\n" +
								 "         \"fkey_info.deferred\" \"DEFERRABILITY\"\n" +
								 "    FROM " + SystemSchema.ForeignKeyInfoTableName + ", " + SystemSchema.ForeignKeyColumnsTableName + "\n" +
								 "   WHERE fkey_info.id = fkey_cols.fk_id\n" +
								 "     AND \"fkey_info.schema\" IN\n" +
								 "              ( SELECT \"name\" FROM " + InformationSchema.ThisUserSchemaInfoViewName + " )\n");

			GrantToPublic(query);
		}

		private static void GrantToPublic(IQuery query) {
			query.Access.GrantOn(DbObjectType.View, InformationSchema.ThisUserSimpleGrantViewName, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.ThisUserGrantViewName, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.ThisUserSchemaInfoViewName, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.ThisUserTableInfoViewName, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.ThisUserTableColumnsViewName, User.PublicName, Privileges.TableRead);

			query.Access.GrantOn(DbObjectType.View, InformationSchema.Catalogs, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.Schemata, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.Tables, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.TablePrivileges, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.Columns, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.ColumnPrivileges, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.PrimaryKeys, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.ImportedKeys, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.ExportedKeys, User.PublicName, Privileges.TableRead);
			query.Access.GrantOn(DbObjectType.View, InformationSchema.CrossReference, User.PublicName, Privileges.TableRead);
		}
	}
}
