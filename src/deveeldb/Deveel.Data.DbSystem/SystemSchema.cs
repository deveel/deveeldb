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
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;

using Deveel.Data.Sql;
using Deveel.Data.Transactions;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// Provides utilities and properties for handling the <c>SYSTEN</c> schema of a database.
	/// </summary>
	/// <remarks>
	/// The <c>SYSTEM</c> schema holds all the core tables and views for making a database system to
	/// be able to work.
	/// </remarks>
	public static class SystemSchema {
		/// <summary>
		/// The name of the system schema that contains tables refering to 
		/// system information.
		/// </summary>
		public const string Name = "SYSTEM";

		/// <summary>
		/// The name of the system schema as <see cref="ObjectName"/>.
		/// </summary>
		public static readonly ObjectName SchemaName = new ObjectName(Name);

		#region Table Names

		/// <summary>
		/// Gets the fully qualified name of the <c>user</c> table.
		/// </summary>
		public static readonly  ObjectName UserTableName = new ObjectName(SchemaName, "user");

		///<summary>
		/// 
		///</summary>
		public static readonly ObjectName SequenceInfoTableName = new ObjectName(SchemaName, "sequence_info");

		///<summary>
		///</summary>
		public static readonly ObjectName SequenceTableName = new ObjectName(SchemaName, "sequence");

		#endregion

		public static readonly TableInfo SequenceInfoTableInfo;
		public static readonly TableInfo SequenceTableInfo;

		static SystemSchema() {
			// SYSTEM.SEQUENCE_INFO
			var tableInfo = new TableInfo(SequenceInfoTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.Numeric());
			tableInfo = tableInfo.AsReadOnly();
			SequenceInfoTableInfo = tableInfo;

			// SYSTEM.SEQUENCE
			tableInfo = new TableInfo(SequenceTableName);
			tableInfo.AddColumn("seq_id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("last_value", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("increment", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("minvalue", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("maxvalue", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("start", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("cache", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("cycle", PrimitiveTypes.Boolean());
			SequenceTableInfo = tableInfo.AsReadOnly();
		}
	}
}
