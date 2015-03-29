using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropDefaultAction : IAlterTableAction {
		public DropDefaultAction(string columnName) {
			ColumnName = columnName;
		}

		public string ColumnName { get; private set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return new DropDefaultAction(ColumnName);
		}

		AlterTableActionType IAlterTableAction.ActionType {
			get { return AlterTableActionType.DropDefault; }
		}
	}
}
