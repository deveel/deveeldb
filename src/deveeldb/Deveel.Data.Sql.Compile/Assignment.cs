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

using Antlr4.Runtime.Misc;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class Assignment {
		public static SqlStatement Statement(PlSqlParser.AssignmentStatementContext context) {
			SqlExpression varRef;

			if (context.general_element() != null) {
				var element = ElementNode.Form(context.general_element());
				if (element.Argument != null &&
					element.Argument.Length > 0)
					throw new ParseCanceledException("Invalid assignment: cannot assign a function");

				var name = element.Id;
				if (name.Parent != null)
					throw new ParseCanceledException("Invalid assignment.");

				varRef = SqlExpression.VariableReference(name.ToString());
			} else if (context.bind_variable() != null) {
				var varName = Name.Variable(context.bind_variable());
				varRef = SqlExpression.VariableReference(varName);
			} else {
				throw new ParseCanceledException("Invalid assignment syntax");
			}

			var valueExp = Expression.Build(context.expression());
			return new AssignVariableStatement(varRef, valueExp);
		}
	}
}
