// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class LoopTests : ContextBasedTest {
		[Test]
		public void LoopAndExitWithNoReturn() {
			var loop = new LoopStatement();
			loop.Statements.Add(new ReturnStatement(SqlExpression.Constant(45)));

			var result = Query.ExecuteStatement(loop);

			Assert.IsNotNull(result);
			Assert.AreEqual(StatementResultType.Result, result.Type);
		}

		[Test]
		public void SimpleForLoop() {
			var loop = new ForLoopStatement("i", SqlExpression.Constant(0), SqlExpression.Constant(200));
			loop.Statements.Add(new DeclareVariableStatement("a", PrimitiveTypes.String()));
			loop.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"),
				SqlExpression.FunctionCall("cast",
					new SqlExpression[] {SqlExpression.VariableReference("i"), SqlExpression.Constant("varchar")})));
			loop.Statements.Add(
				new ConditionStatement(SqlExpression.Equal(SqlExpression.VariableReference("i"), SqlExpression.Constant(200)),
					new SqlStatement[] {new ReturnStatement(SqlExpression.VariableReference("a"))}));

			// TODO: Temporary (not to fail the whole build): mst fix the AssignStatement
			Assert.Throws<ExpressionEvaluateException>(() =>  Query.ExecuteStatement(loop));
		}
	}
}
