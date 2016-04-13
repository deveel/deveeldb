using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	class PlSqlBody : SqlStatement {
		public PlSqlBody() {
			Statements = new List<SqlStatement>();
			ExceptionHandlers = new List<ExceptionHandler>();
		}

		public List<SqlStatement> Statements { get; private set; }
		
		public List<ExceptionHandler> ExceptionHandlers { get; private set; }		
	}
}
