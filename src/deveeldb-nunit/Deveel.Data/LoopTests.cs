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
	}
}
