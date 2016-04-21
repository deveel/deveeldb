using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CreateFunctionTests : SqlCompileTestBase {
		[Test]
		public void PlSqlFunctionReturnsInteger() {
			const string sql = @"CREATE OR REPLACE FUNCTION APP.func1(a INT, b VARCHAR NOT NULL)
                                     RETURN INTEGER IS
                                 BEGIN
                                     c INT := a + CAST(b AS INTEGER);
                                     RETURN :c;
                                 END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateFunctionStatement>(statement);
		}

		[Test]
		public void ExternFunctionReturnsInterger() {
			const string sql = @"CREATE OR REPLACE FUNCTION APP.func1(a INT, b VARCHAR NOT NULL)
                                     RETURN INTEGER IS LANGUAGE DOTNET 'Deveel.Data.Sql.Compile.CreteFucntionTest+TestClass.Func1'";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateExternalFunctionStatement>(statement);

			var createFunction = (CreateExternalFunctionStatement) statement;

			Assert.IsNotNull(createFunction.ExternalReference);
			Assert.AreEqual("Deveel.Data.Sql.Compile.CreteFucntionTest+TestClass.Func1", createFunction.ExternalReference);
		}
	}
}
