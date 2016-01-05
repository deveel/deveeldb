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
		public DropTableStatement(ObjectName tableName) 
			: this(tableName, false) {
		}

		public DropTableStatement(ObjectName tableName, bool ifExists) {
			if (tableName == null)
				throw new ArgumentNullException("tableNames");

			TableName = tableName;
			IfExists = ifExists;
		}

		public ObjectName TableName { get; private set; }

		public bool IfExists { get; set; }

		IStatement IPreparableStatement.Prepare(IRequest context) {
			var tableName = context.Query.ResolveTableName(TableName);
			if (!context.Query.TableExists(tableName))
				throw new ObjectNotFoundException(TableName);

			return new Prepared(tableName, IfExists);
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			public Prepared(ObjectName tableName, bool ifExists) {
				TableName = tableName;
				IfExists = ifExists;
			}

			private Prepared(ObjectData data) {
				TableName = data.GetValue<ObjectName>("TableName");
				IfExists = data.GetBoolean("IfExists");
			}

			public ObjectName TableName { get; private set; }

			public bool IfExists { get; private set; }

			protected override void GetData(SerializeData data) {
				data.SetValue("TableName", TableName);
				data.SetValue("IfExists", IfExists);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				context.Request.Query.DropTable(TableName, IfExists);
			}
		}

		#endregion
	}
}
