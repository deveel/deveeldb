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
using Deveel.Data.Sql;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data {
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


		public static void CreateViews(IQueryContext context) {
			context.ExecuteQuery("CREATE VIEW INFORMATION_SCHEMA.ThisUserSimpleGrant AS " +
			                     "  SELECT \"priv_bit\", \"object\", \"name\", \"user\", " +
			                     "         \"grant_option\", \"granter\" " +
			                     "    FROM " + SystemSchema.UserGrantsTableName +
			                     "   WHERE ( user = user() OR user = '@PUBLIC' )");

			context.ExecuteQuery("CREATE VIEW INFORMATION_SCHEMA.ThisUserGrant AS " +
			                     "  SELECT \"description\", \"object\", \"name\", \"user\", " +
			                     "         \"grant_option\", \"granter\" " +
			                     "    FROM " + SystemSchema.UserGrantsTableName + ", " + SystemSchema.PrivilegesTableName +
			                     "   WHERE ( user = user() OR user = '@PUBLIC' )" +
			                     "     AND " + SystemSchema.UserGrantsTableName + ".priv_bit = " +
			                     SystemSchema.PrivilegesTableName + ".priv_bit");

			context.ExecuteQuery("CREATE VIEW INFORMATION_SCHEMA.ThisUserSchemaInfo AS " +
			                     "  SELECT * FROM  " + SystemSchema.SchemaInfoTableName +
			                     "   WHERE \"name\" IN ( " +
			                     "     SELECT \"name\" " +
			                     "       FROM INFORMATION_SCHEMA.ThisUserGrant " +
			                     "      WHERE \"object\" = " + ((int)DbObjectType.Schema) +
			                     "        AND \"description\" = '" + Privileges.List + "' )");

			context.ExecuteQuery("CREATE VIEW INFORMATION_SCHEMA.ThisUserTableColumns AS " +
			                     "  SELECT * FROM " + SystemSchema.TableColumnsTableName +
			                     "   WHERE \"schema\" IN ( " +
			                     "     SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )");
		}
	}
}
