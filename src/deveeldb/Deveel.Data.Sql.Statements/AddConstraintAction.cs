using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AddConstraintAction : IAlterTableAction {
		public AddConstraintAction(ConstraintInfo constraint) {
			if (constraint == null)
				throw new ArgumentNullException("constraint");

			Constraint = constraint;
		}

		public ConstraintInfo Constraint { get; private set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			ConstraintInfo constraint;

			if (Constraint.ConstraintType == ConstraintType.Check) {
				var exp = Constraint.CheckExpression;
				if (exp != null)
					exp = exp.Prepare(preparer);

				constraint = ConstraintInfo.Check(Constraint.ConstraintName, Constraint.TableName, exp, Constraint.ColumnNames);
			} else if (Constraint.ConstraintType == ConstraintType.Unique) {
				constraint = ConstraintInfo.Unique(Constraint.ConstraintName, Constraint.TableName, Constraint.ColumnNames);
			} else if (Constraint.ConstraintType == ConstraintType.PrimaryKey) {
				constraint = ConstraintInfo.PrimaryKey(Constraint.ConstraintName, Constraint.TableName, Constraint.ColumnNames);
			} else if (Constraint.ConstraintType == ConstraintType.ForeignKey) {
				constraint = ConstraintInfo.ForeignKey(Constraint.ConstraintName, Constraint.TableName, Constraint.ColumnNames,
					Constraint.ForeignTable, Constraint.ForeignColumnNames);
				constraint.OnDelete = Constraint.OnDelete;
				constraint.OnUpdate = Constraint.OnUpdate;
			} else {
				throw new InvalidOperationException();
			}

			return new AddConstraintAction(constraint);
		}

		AlterTableActionType IAlterTableAction.ActionType {
			get { return AlterTableActionType.AddConstraint; }
		}
	}
}
