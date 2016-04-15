using System;

using Deveel.Data.Sql.Compile;

namespace Deveel.Data.Sql.Statements {
	class LoopBreakChecker : StatementVisitor {
		private bool breakFound;
		private string label;

		public bool Verify(LoopStatement statement) {
			label = statement.Label;

			VisitStatement(statement);
			return breakFound;
		}

		public static bool HasBreak(LoopStatement statement) {
			var visitor = new LoopBreakChecker();
			return visitor.Verify(statement);
		}

		protected override SqlStatement VisitReturn(ReturnStatement statement) {
			breakFound = true;
			return base.VisitReturn(statement);
		}

		protected override SqlStatement VisitGoTo(GoToStatement statement) {
			breakFound = true;
			return base.VisitGoTo(statement);
		}

		protected override SqlStatement VisitExit(ExitStatement statement) {
			if (!String.Equals(label, statement.Label))
				breakFound = true;

			return base.VisitExit(statement);
		}

		protected override SqlStatement VisitContinue(ContinueStatement statement) {
			if (!String.Equals(label, statement.Label))
				breakFound = true;

			return base.VisitContinue(statement);
		}
	}
}
