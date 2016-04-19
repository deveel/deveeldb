// 
//  Copyright 2010-2016 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


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
