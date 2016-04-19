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
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class LoopControlBuilder {
		public static LoopControlStatement Build(PlSqlParser.ExitStatementContext context) {
			return Build(LoopControlType.Exit, context.labelName(), context.condition());
		}

		public static LoopControlStatement Build(PlSqlParser.ContinueStatementContext context) {
			return Build(LoopControlType.Continue, context.labelName(), context.condition());
		}

		private static LoopControlStatement Build(LoopControlType controlType, PlSqlParser.LabelNameContext labelContext, PlSqlParser.ConditionContext conditionContext) {
			string label = null;
			SqlExpression whenExpression = null;

			if (labelContext != null)
				label = Name.Simple(labelContext);

			if (conditionContext != null)
				whenExpression = Expression.Build(conditionContext.expression());

			return new LoopControlStatement(controlType, label, whenExpression);
		}
	}
}
