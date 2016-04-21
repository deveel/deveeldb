using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CreateProcedureTests : SqlCompileTestBase {
		[Test]
		public void PlSqlFunctionReturnsInteger() {
			const string sql = @"CREATE OR REPLACE PROCEDURE APP.proc1(a INT, b VARCHAR NOT NULL) AS
                                 BEGIN
                                     FOR i IN 0..20 LOOP
                                         c VARCHAR;
                                         SELECT name INTO :c FROM table1 WHERE id > a; 
                                     END LOOP
                                 END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateProcedureStatement>(statement);
		}

		[Test]
		public void ExternFunctionReturnsInterger() {
			const string sql = @"CREATE OR REPLACE PROCEDURE APP.proc1(a INT, b VARCHAR NOT NULL)
                                     IS LANGUAGE DOTNET 'Deveel.Data.Sql.Compile.CreteFucntionTest+TestClass.Func1'";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateExternalProcedureStatement>(statement);

			var createFunction = (CreateExternalProcedureStatement)statement;

			Assert.IsNotNull(createFunction.ExternalReference);
			Assert.AreEqual("Deveel.Data.Sql.Compile.CreteFucntionTest+TestClass.Func1", createFunction.ExternalReference);
		}
	}
}
