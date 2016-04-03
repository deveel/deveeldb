using System;
using System.Reflection;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Mapping {
	public sealed class ConstraintMapInfo {
		internal ConstraintMapInfo(MemberInfo member, string columnName, ConstraintType constraintType, string checkExpression) {
			Member = member;
			ColumnName = columnName;
			ConstraintType = constraintType;
			CheckExpression = checkExpression;
		}

		public MemberInfo Member { get; private set; }

		public string ColumnName { get; private set; }

		public ConstraintType ConstraintType { get; private set; }

		public string CheckExpression { get; private set; }

		internal AddConstraintAction AsAddConstraintAction() {
			var constraint = new SqlTableConstraint(ConstraintType, new []{ColumnName});
			if (ConstraintType == ConstraintType.Check &&
			    !String.IsNullOrEmpty(CheckExpression)) {
				constraint.CheckExpression = SqlExpression.Parse(CheckExpression);
			}

			// TODO: Implement the foreign keys

			return new AddConstraintAction(constraint);
		}
	}
}
