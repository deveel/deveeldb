using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class OpenStatement : SqlNonPreparableStatement {
		public OpenStatement(string cursorName) 
			: this(cursorName, new SqlExpression[] {}) {
		}

		public OpenStatement(string cursorName, SqlExpression[] arguments) {
			CursorName = cursorName;
			Arguments = arguments;
		}

		public string CursorName { get; private set; }

		public SqlExpression[] Arguments { get; set; }

		public override ITable Execute(IQueryContext context) {
			
			return FunctionTable.ResultTable(context, 0);
		}
	}
}
