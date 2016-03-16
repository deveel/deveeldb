// 
//  Copyright 2010-2015 Deveel
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
//


using System;

using Deveel.Data.Security;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Schemas {
	public static class InformationSchema {
		public const string SchemaName = "INFORMATION_SCHEMA";

		public static readonly ObjectName Name = new ObjectName(SchemaName);

		public static readonly ObjectName Catalogs = new ObjectName(Name, "catalogs");

		public static readonly ObjectName Tables = new ObjectName(Name, "tables");

		public static readonly ObjectName TablePrivileges = new ObjectName(Name, "table_privileges");

		public static readonly ObjectName Schemata = new ObjectName(Name, "schemata");

		public static readonly ObjectName Columns = new ObjectName(Name, "columns");

		public static readonly ObjectName ColumnPrivileges = new ObjectName(Name, "column_privileges");

		public static readonly ObjectName PrimaryKeys = new ObjectName(Name, "primary_keys");

		public static readonly ObjectName ImportedKeys = new ObjectName(Name, "imported_keys");

		public static readonly ObjectName ExportedKeys = new ObjectName(Name, "exported_keys");

		public static readonly ObjectName DataTypes = new ObjectName(Name, "data_types");

		public static readonly ObjectName CrossReference = new ObjectName(Name, "cross_reference");

		public static readonly ObjectName UserPrivileges = new ObjectName(Name, "user_privileges");

		public static readonly  ObjectName ThisUserSimpleGrantViewName = new ObjectName(Name, "ThisUserSimpleGrant");

		public static readonly ObjectName ThisUserGrantViewName = new ObjectName(Name, "ThisUserGrant");

		public static readonly ObjectName ThisUserSchemaInfoViewName = new ObjectName(Name, "ThisUserSchemaInfo");

		public static readonly ObjectName ThisUserTableColumnsViewName = new ObjectName(Name, "ThisUserTableColumns");

		public static readonly ObjectName ThisUserTableInfoViewName = new ObjectName(Name, "ThisUserTableInfo");


		public static void CreateViews(IQuery query) {
			// This view shows the grants that the user has (no join, only priv_bit).
			query.ExecuteQuery("CREATE VIEW " + ThisUserSimpleGrantViewName + " AS " +
			                     "  SELECT \"priv_bit\", \"object\", \"name\", \"user\", " +
			                     "         \"grant_option\", \"granter\" " +
			                     "    FROM " + SystemSchema.UserGrantsTableName +
			                     "   WHERE ( user = user() OR user = '@PUBLIC' )");

			// This view shows the grants that the user is allowed to see
			query.ExecuteQuery("CREATE VIEW " + ThisUserGrantViewName + " AS " +
			                     "  SELECT \"description\", \"object\", \"name\", \"user\", " +
			                     "         \"grant_option\", \"granter\" " +
			                     "    FROM " + SystemSchema.UserGrantsTableName + ", " + SystemSchema.PrivilegesTableName +
			                     "   WHERE ( user = user() OR user = '@PUBLIC' )" +
			                     "     AND " + SystemSchema.UserGrantsTableName + ".priv_bit = " +
			                     SystemSchema.PrivilegesTableName + ".priv_bit");

			// A view that represents the list of schema this user is allowed to view
			// the contents of.
			query.ExecuteQuery("CREATE VIEW " + ThisUserSchemaInfoViewName + " AS " +
			                     "  SELECT * FROM  " + SystemSchema.SchemaInfoTableName +
			                     "   WHERE \"name\" IN ( " +
			                     "     SELECT \"name\" " +
			                     "       FROM " + ThisUserGrantViewName + " " +
			                     "      WHERE \"object\" = " + ((int)DbObjectType.Schema) +
			                     "        AND \"description\" = '" + Privileges.List + "' )");

			// A view that exposes the table_columns table but only for the tables
			// this user has read access to.
			query.ExecuteQuery("CREATE VIEW " + ThisUserTableColumnsViewName + " AS " +
			                     "  SELECT * FROM " + SystemSchema.TableColumnsTableName +
			                     "   WHERE \"schema\" IN ( " +
			                     "     SELECT \"name\" FROM " + ThisUserSchemaInfoViewName + ")");

			// A view that exposes the 'table_info' table but only for the tables
			// this user has read access to.
			query.ExecuteQuery("CREATE VIEW " + ThisUserTableInfoViewName + " AS " +
			                     "  SELECT * FROM " + SystemSchema.TableInfoTableName +
			                     "   WHERE \"schema\" IN ( " +
			                     "     SELECT \"name\" FROM "+ThisUserSchemaInfoViewName + ")");

			query.ExecuteQuery("  CREATE VIEW " + Tables + " AS " +
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
			                     "    FROM " + ThisUserTableInfoViewName + "\n");

			query.ExecuteQuery("  CREATE VIEW " + Schemata + " AS " +
			                     "  SELECT \"name\" AS \"TABLE_SCHEMA\", \n" +
			                     "         NULL AS \"TABLE_CATALOG\" \n" +
			                     "    FROM " + ThisUserSchemaInfoViewName + "\n");

			query.ExecuteQuery("  CREATE VIEW " + Catalogs + " AS " +
			                     "  SELECT NULL AS \"TABLE_CATALOG\" \n" +
			                     "    FROM " + SystemSchema.SchemaInfoTableName + "\n" + // Hacky, this will generate a 0 row
			                     "   WHERE FALSE\n");

			query.ExecuteQuery("  CREATE VIEW " + Columns + " AS " +
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
			                     "    FROM " + ThisUserTableColumnsViewName + "\n");

			query.ExecuteQuery("  CREATE VIEW " + ColumnPrivileges + " AS " +
			                     "  SELECT \"TABLE_CATALOG\",\n" +
			                     "         \"TABLE_SCHEMA\",\n" +
			                     "         \"TABLE_NAME\",\n" +
			                     "         \"COLUMN_NAME\",\n" +
			                     "         IIF(\"ThisUserGrant.granter\" = '@SYSTEM', \n" +
			                     "                        NULL, \"ThisUserGrant.granter\") AS \"GRANTOR\",\n" +
			                     "         IIF(\"ThisUserGrant.user\" = '@PUBLIC', \n" +
			                     "                    'public', \"ThisUserGrant.user\") AS \"GRANTEE\",\n" +
			                     "         \"ThisUserGrant.description\" AS \"PRIVILEGE\",\n" +
			                     "         IIF(\"grant_option\" = 'true', 'YES', 'NO') AS \"IS_GRANTABLE\" \n" +
			                     "    FROM " + Columns + ", INFORMATION_SCHEMA.ThisUserGrant \n" +
			                     "   WHERE CONCAT(columns.TABLE_SCHEMA, '.', columns.TABLE_NAME) = \n" +
			                     "         ThisUserGrant.name \n" +
			                     "     AND INFORMATION_SCHEMA.ThisUserGrant.object = 1 \n" +
			                     "     AND INFORMATION_SCHEMA.ThisUserGrant.description IS NOT NULL \n");

			query.ExecuteQuery("  CREATE VIEW " + TablePrivileges + " AS " +
			                     "  SELECT \"TABLE_CATALOG\",\n" +
			                     "         \"TABLE_SCHEMA\",\n" +
			                     "         \"TABLE_NAME\",\n" +
			                     "         IIF(\"ThisUserGrant.granter\" = '@SYSTEM', \n" +
			                     "                        NULL, \"ThisUserGrant.granter\") AS \"GRANTOR\",\n" +
			                     "         IIF(\"ThisUserGrant.user\" = '@PUBLIC', \n" +
			                     "                    'public', \"ThisUserGrant.user\") AS \"GRANTEE\",\n" +
			                     "         \"ThisUserGrant.description\" AS \"PRIVILEGE\",\n" +
			                     "         IIF(\"grant_option\" = 'true', 'YES', 'NO') AS \"IS_GRANTABLE\" \n" +
			                     "    FROM " + Tables + ", INFORMATION_SCHEMA.ThisUserGrant \n" +
			                     "   WHERE CONCAT(tables.TABLE_SCHEMA, '.', tables.TABLE_NAME) = \n" +
			                     "         ThisUserGrant.name \n" +
			                     "     AND INFORMATION_SCHEMA.ThisUserGrant.object = 1 \n" +
			                     "     AND INFORMATION_SCHEMA.ThisUserGrant.description IS NOT NULL \n");

			query.ExecuteQuery("  CREATE VIEW " + PrimaryKeys + " AS " +
			                     "  SELECT NULL \"TABLE_CATALOG\",\n" +
			                     "         \"schema\" \"TABLE_SCHEMA\",\n" +
			                     "         \"table\" \"TABLE_NAME\",\n" +
			                     "         \"column\" \"COLUMN_NAME\",\n" +
			                     "         \"SYSTEM.pkey_cols.seq_no\" \"KEY_SEQ\",\n" +
			                     "         \"name\" \"PK_NAME\"\n" +
			                     "    FROM " + SystemSchema.PrimaryKeyInfoTableName + ", " + SystemSchema.PrimaryKeyColumnsTableName + "\n" +
			                     "   WHERE pkey_info.id = pkey_cols.pk_id\n" +
			                     "     AND \"schema\" IN\n" +
			                     "            ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n");

			query.ExecuteQuery("  CREATE VIEW " + ImportedKeys + " AS " +
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

			query.ExecuteQuery("  CREATE VIEW " + ExportedKeys + " AS " +
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
			                     "              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n");

			query.ExecuteQuery("  CREATE VIEW " + CrossReference + " AS " +
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
			                     "              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n");
		}

		public static void GrantToPublic(IQuery query) {
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, ThisUserSimpleGrantViewName, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, ThisUserGrantViewName, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, ThisUserSchemaInfoViewName, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, ThisUserTableInfoViewName, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, ThisUserTableColumnsViewName, User.PublicName, Privileges.TableRead);

			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, Catalogs, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, Schemata, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, Tables, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, TablePrivileges, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, Columns, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, ColumnPrivileges, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, PrimaryKeys, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, ImportedKeys, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, ExportedKeys, User.PublicName, Privileges.TableRead);
			query.Access.SessionAccess.GrantToUserOn(DbObjectType.View, CrossReference, User.PublicName, Privileges.TableRead);
		}
	}
}
