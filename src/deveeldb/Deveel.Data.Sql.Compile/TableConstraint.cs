using System;
using System.Linq;

using Antlr4.Runtime.Misc;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Compile {
	static class TableConstraint {
		public static SqlTableConstraint Form(PlSqlParser.TableConstraintContext context) {
			string constraintName = Name.Simple(context.id());

			ConstraintType type;
			string[] columns = null;
			string refTable = null;
			string[] refColumns = null;
			SqlExpression checkExp =  null;

			if (context.primaryKeyConstraint() != null) {
				type = ConstraintType.PrimaryKey;

				columns = context.primaryKeyConstraint().columnList().columnName().Select(Name.Simple).ToArray();
			} else if (context.uniqueKeyConstraint() != null) {
				type = ConstraintType.Unique;
				columns = context.uniqueKeyConstraint().columnList().columnName().Select(Name.Simple).ToArray();
			} else if (context.checkConstraint() != null) {
				type = ConstraintType.Check;
				checkExp = Expression.Build(context.checkConstraint().expression());
			} else if (context.foreignKeyConstraint() != null) {
				type = ConstraintType.ForeignKey;
				columns = context.foreignKeyConstraint().columns.columnName().Select(Name.Simple).ToArray();
				refColumns = context.foreignKeyConstraint().refColumns.columnName().Select(Name.Simple).ToArray();
				refTable = Name.Object(context.foreignKeyConstraint().objectName()).ToString();
			} else {
				throw new ParseCanceledException("Invalid ");
			}

			var constraint = new SqlTableConstraint(constraintName, type, columns);

			if (type == ConstraintType.ForeignKey) {
				constraint.ReferenceTable = refTable;
				constraint.ReferenceColumns = refColumns;
			} else if (type == ConstraintType.Check) {
				constraint.CheckExpression = checkExp;
			}

			return constraint;
		}
	}
}