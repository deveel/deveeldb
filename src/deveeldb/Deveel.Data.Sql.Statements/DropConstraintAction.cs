using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropConstraintAction : IAlterTableAction {
		public DropConstraintAction(string constraintName) {
			if (String.IsNullOrEmpty(constraintName))
				throw new ArgumentNullException("constraintName");

			ConstraintName = constraintName;
		}

		public string ConstraintName { get; private set; }

		AlterTableActionType IAlterTableAction.ActionType {
			get { return AlterTableActionType.DropConstraint; }
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return new DropConstraintAction(ConstraintName);
		}
	}
}
