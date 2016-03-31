// 
//  Copyright 2010-2016 Deveel
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
	}
}
