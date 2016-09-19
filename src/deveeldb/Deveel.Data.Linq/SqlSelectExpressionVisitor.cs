using System;
using System.Linq.Expressions;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Parsing;

namespace Deveel.Data.Linq {
	class SqlSelectExpressionVisitor : RelinqExpressionVisitor {
		private string lastSource;
		private ExpressionCompileContext compileContext;

		public SqlSelectExpressionVisitor(ExpressionCompileContext compileContext) {
			this.compileContext = compileContext;
		}

		protected override Expression VisitMember(MemberExpression expression) {
			var type = expression.Member.ReflectedType;
			var memberName = expression.Member.Name;

			var mapInfo = compileContext.GetMemberMap(type, memberName);

			var columnName = new ObjectName(mapInfo.ColumnName);

			if (!String.IsNullOrEmpty(lastSource))
				columnName = new ObjectName(new ObjectName(lastSource), columnName.FullName);

			compileContext.Columns.Add(new SelectColumn(SqlExpression.Reference(columnName)));

			return expression;
		}

		protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression) {
			lastSource = expression.ReferencedQuerySource.ItemName;
			return expression;
		}
	}
}
