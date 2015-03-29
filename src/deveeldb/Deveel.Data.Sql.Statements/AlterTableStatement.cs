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
