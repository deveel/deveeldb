using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SetDefaultAction : IAlterTableAction {
		public SetDefaultAction(string columnName, SqlExpression defaultExpression) {
			ColumnName = columnName;
			DefaultExpression = defaultExpression;
		}

		public string ColumnName { get; private set; }

		public SqlExpression DefaultExpression { get; private set; }

		AlterTableActionType IAlterTableAction.ActionType {
			get { return AlterTableActionType.SetDefault; }
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var defaultExp = DefaultExpression;
			if (defaultExp != null)
				defaultExp = defaultExp.Prepare(preparer);

			return new SetDefaultAction(ColumnName, defaultExp);
		}
	}
}
