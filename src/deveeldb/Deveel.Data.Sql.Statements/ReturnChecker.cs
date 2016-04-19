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
