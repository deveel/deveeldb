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

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class DropTableStatement : SqlStatement, IPreparableStatement {
		public DropTableStatement(string[] tableNames) 
			: this(tableNames, false) {
		}

		public DropTableStatement(string[] tableNames, bool ifExists) {
			if (tableNames == null)
				throw new ArgumentNullException("tableNames");
			if (tableNames.Length == 0)
				throw new ArgumentException("The table name list cannot be empty", "tableNames");

			if (tableNames.Any(String.IsNullOrEmpty))
				throw new ArgumentException("One of the specified table names is null.");

			TableNames = tableNames;
			IfExists = ifExists;
		}

		public DropTableStatement(string tableName)
			: this(tableName, false) {
		}

		public DropTableStatement(string tableName, bool ifExists)
			: this(new[] {tableName}, ifExists) {
		}

		public string[] TableNames { get; private set; }

		public bool IfExists { get; set; }

		IStatement IPreparableStatement.Prepare(IRequest context) {
			var tableNameList = TableNames.ToList();
			var dropTables = new List<string>();

			foreach (var tableName in tableNameList) {
				if (dropTables.Contains(tableName, StringComparer.OrdinalIgnoreCase))
					throw new StatementPrepareException(String.Format("Duplicated table name '{0}' in the list of tables to drop.",
						tableName));

				dropTables.Add(tableName);
			}

			var resolvedTableNames = dropTables.Select(context.Query.ResolveTableName);

			return new Prepared(resolvedTableNames.ToArray(), IfExists);
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			public Prepared(ObjectName[] tableNames, bool ifExists) {
				TableNames = tableNames;
				IfExists = ifExists;
			}

			private Prepared(ObjectData data) {
				TableNames = data.GetValue<ObjectName[]>("TableNames");
				IfExists = data.GetBoolean("IfExists");
			}

			public ObjectName[] TableNames { get; private set; }

			public bool IfExists { get; private set; }

			protected override void GetData(SerializeData data) {
				data.SetValue("TableNames", TableNames);
				data.SetValue("IfExists", IfExists);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				context.Request.Query.DropTables(TableNames, IfExists);
			}
		}

		#endregion
	}
}
