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
using System.Data;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AlterTableStatement : Statement {
		public AlterTableStatement(ObjectName tableName) 
			: this(tableName, (IAlterTableAction) null) {
		}

		public AlterTableStatement(ObjectName tableName, IAlterTableAction action)
			: this(tableName, new[] {action}) {
		}

		public AlterTableStatement(ObjectName tableName, IEnumerable<IAlterTableAction> actions) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TableName = tableName;
			Actions = new List<IAlterTableAction>();

			if (actions != null) {
				foreach (var action in actions) {
					Actions.Add(action);
				}
			}
		}

		public override StatementType StatementType {
			get { return StatementType.AlterTable; }
		}

		public ObjectName TableName { get; private set; }

		public IList<IAlterTableAction> Actions { get; private set; } 

		protected override PreparedStatement PrepareStatement(IQueryContext context) {
			throw new NotImplementedException();
		}

		#region AlterTablePreparedStatement

		[Serializable]
		class AlterTablePreparedStatement : PreparedStatement {
			public override ITable Evaluate(IQueryContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion

		#region Keys

		internal static class Keys {
			public const string TableName = "TableName";
			public const string Actions = "Actions";
		}

		#endregion
	}
}
