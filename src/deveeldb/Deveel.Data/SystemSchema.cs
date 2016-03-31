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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;
using Deveel.Data.Sql.Types;
using Deveel.Data.Util;

namespace Deveel.Data {
	/// <summary>
	/// Provides utilities and properties for handling the <c>SYSTEN</c> schema of a database.
	/// </summary>
	/// <remarks>
	/// The <c>SYSTEM</c> schema holds all the core tables and views for making a database system to
	/// be able to work.
	/// </remarks>
	public static class SystemSchema {
		/// <summary>
		/// The name of the system schema that contains tables referring to 
		/// system information.
		/// </summary>
		public const string Name = "SYSTEM";

		/// <summary>
		/// The name of the system schema as <see cref="ObjectName"/>.
		/// </summary>
		public static readonly ObjectName SchemaName = new ObjectName(Name);

		#region Table Names

		public static readonly ObjectName SchemaInfoTableName = new ObjectName(SchemaName, "schema_info");

		public static readonly  ObjectName TableInfoTableName = new ObjectName(SchemaName, "table_info");

		public static readonly ObjectName TableColumnsTableName = new ObjectName(SchemaName, "table_cols");

		public static readonly ObjectName VariablesTableName = new ObjectName(SchemaName, "vars");

		public static readonly ObjectName ProductInfoTableName = new ObjectName(SchemaName, "product_info");

		public static readonly ObjectName StatisticsTableName = new ObjectName(SchemaName, "stats");

		public static readonly ObjectName PrimaryKeyInfoTableName = new ObjectName(SchemaName, "pkey_info");

		public static readonly ObjectName PrimaryKeyColumnsTableName = new ObjectName(SchemaName, "pkey_cols");

		public static readonly ObjectName ForeignKeyInfoTableName = new ObjectName(SchemaName, "fkey_info");

		public static readonly ObjectName ForeignKeyColumnsTableName = new ObjectName(SchemaName, "fkey_cols");

		public static readonly  ObjectName UniqueKeyInfoTableName = new ObjectName(SchemaName, "unique_info");

		public static readonly ObjectName UniqueKeyColumnsTableName = new ObjectName(SchemaName, "unique_cols");

		public static readonly ObjectName CheckInfoTableName = new ObjectName(SchemaName, "check_info");

		public static readonly ObjectName OldTriggerTableName = new ObjectName(SchemaName, "OLD");

		public static readonly ObjectName NewTriggerTableName = new ObjectName(SchemaName, "NEW");

		public static readonly ObjectName SqlTypesTableName = new ObjectName(SchemaName, "sql_types");

		public static readonly ObjectName SessionInfoTableName = new ObjectName(SchemaName, "session_info");

		public static readonly ObjectName OpenSessionsTableName = new ObjectName(SchemaName, "open_sessions");

		public static readonly ObjectName PrivilegesTableName = new ObjectName(SchemaName, "privs");

		public static readonly ObjectName GrantsTableName = new ObjectName(SchemaName, "grants");

		#endregion
	}
}
