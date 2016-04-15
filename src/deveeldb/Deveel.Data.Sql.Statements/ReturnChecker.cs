using System;

namespace Deveel.Data.Sql.Statements {
	// TODO: A more advanced version of this should analyze the paths of the statement and check if all the paths return
	class ReturnChecker : StatementVisitor {
		private bool returnFound;

		public bool Verify(SqlStatement statement) {
			VisitStatement(statement);
			return returnFound;
		}

		protected override SqlStatement VisitReturn(ReturnStatement statement) {
			returnFound = true;
			return base.VisitReturn(statement);
		}

		public static bool HasReturn(SqlStatement statement) {
			var visitor = new ReturnChecker();
			return visitor.Verify(statement);
		}
	}
}
