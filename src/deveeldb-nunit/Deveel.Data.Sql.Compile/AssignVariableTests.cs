using System;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class AssignVariableTests : SqlCompileTestBase {
		[Test]
		public void MarkedVariableToConstant() {
			const string sql = ":a := 23";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<AssignVariableStatement>(statement);

			var assignVar = (AssignVariableStatement) statement;

			Assert.IsInstanceOf<SqlVariableReferenceExpression>(assignVar.VariableReference);
			Assert.IsInstanceOf<SqlConstantExpression>(assignVar.ValueExpression);
		}


		[Test]
		public void UnmarkedVariableToConstant() {
			const string sql = "a := 8779.90";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<AssignVariableStatement>(statement);

			var assignVar = (AssignVariableStatement)statement;

			Assert.IsInstanceOf<SqlReferenceExpression>(assignVar.VariableReference);
			Assert.IsInstanceOf<SqlConstantExpression>(assignVar.ValueExpression);
		}
	}
}