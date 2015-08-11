using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements {
	public sealed class DeclareCursorStatement : SqlNonPreparableStatement {
		public DeclareCursorStatement(string cursorName, SqlQueryExpression queryExpression) 
			: this(cursorName, null, queryExpression) {
		}

		public DeclareCursorStatement(string cursorName, IEnumerable<CursorParameter> parameters, SqlQueryExpression queryExpression) 
			: this(cursorName, parameters, CursorFlags.Insensitive, queryExpression) {
		}

		public DeclareCursorStatement(string cursorName, CursorFlags flags, SqlQueryExpression queryExpression) 
			: this(cursorName, null, flags, queryExpression) {
		}

		public DeclareCursorStatement(string cursorName, IEnumerable<CursorParameter> parameters, CursorFlags flags, SqlQueryExpression queryExpression) {
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			CursorName = cursorName;
			Parameters = parameters;
			Flags = flags;
			QueryExpression = queryExpression;
		}

		public string CursorName { get; private set; }

		public SqlQueryExpression QueryExpression { get; private set; }

		public CursorFlags Flags { get; set; }

		public IEnumerable<CursorParameter> Parameters { get; set; } 

		public override ITable Execute(IQueryContext context) {
			var cursorInfo = new CursorInfo(CursorName, Flags, QueryExpression);
			if (Parameters != null) {
				foreach (var parameter in Parameters) {
					cursorInfo.Parameters.Add(parameter);
				}
			}

			context.DeclareCursor(cursorInfo);
			return FunctionTable.ResultTable(context, 0);
		}
	}
}
