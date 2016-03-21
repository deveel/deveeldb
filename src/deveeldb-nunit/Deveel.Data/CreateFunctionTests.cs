using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateFunctionTests : ContextBasedTest {
		[Test]
		public void SimpleReturn() {
			var body = new PlSqlBlockStatement();

			body.Declarations.Add(new DeclareVariableStatement("a", PrimitiveTypes.Integer()));
			body.Statements.Add(new ReturnStatement(SqlExpression.VariableReference("a")));

			var funName = ObjectName.Parse("APP.fun1");
			Query.CreateFunction(funName, PrimitiveTypes.Numeric(), body);

			// TODO: assert it exists
		}
	}
}
