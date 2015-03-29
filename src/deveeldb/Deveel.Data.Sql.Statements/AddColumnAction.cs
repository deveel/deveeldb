using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AddColumnAction : IAlterTableAction {
		public AddColumnAction(ColumnInfo column) {
			if (column == null)
				throw new ArgumentNullException("column");

			Column = column;
		}

		public ColumnInfo Column { get; private set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var newColumn = new ColumnInfo(Column.ColumnName, Column.ColumnType) {
				IsNotNull = Column.IsNotNull
			};

			var defaultExp = Column.DefaultExpression;
			if (defaultExp != null)
				newColumn.DefaultExpression = defaultExp.Prepare(preparer);

			return new AddColumnAction(newColumn);
		}

		AlterTableActionType IAlterTableAction.ActionType {
			get { return AlterTableActionType.AddColumn; }
		}
	}
}
