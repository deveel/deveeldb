using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropColumnAction : IAlterTableAction {
		public DropColumnAction(string columnName) {
			ColumnName = columnName;
		}

		public string ColumnName { get; private set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return new DropColumnAction(ColumnName);
		}

		AlterTableActionType IAlterTableAction.ActionType {
			get { return AlterTableActionType.DropColumn; }
		}
	}
}
