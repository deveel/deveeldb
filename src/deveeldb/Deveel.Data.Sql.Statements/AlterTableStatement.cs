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
			: this(tableName, null) {
		}

		public AlterTableStatement(ObjectName tableName, IEnumerable<IAlterTableAction> actions) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TableName = tableName;

			if (actions != null) {
				foreach (var action in actions) {
					Actions.Add(action);
				}
			}
		}

		internal AlterTableStatement() {
		}

		public ObjectName TableName {
			get { return GetValue<ObjectName>(Keys.TableName); }
			private set { SetValue(Keys.TableName, value); }
		}

		public IList<IAlterTableAction> Actions {
			get { return GetList<IAlterTableAction>(Keys.Actions); }
		} 

		protected override PreparedStatement OnPrepare(IQueryContext context) {
			throw new NotImplementedException();
		}

		#region AlterTablePreparedStatement

		class AlterTablePreparedStatement : PreparedStatement {
			protected override ITable OnEvaluate(IQueryContext context) {
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
