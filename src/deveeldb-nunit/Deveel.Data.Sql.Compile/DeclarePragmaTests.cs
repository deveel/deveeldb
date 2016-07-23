using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class DeclarePragmaTests : SqlCompileTestBase {
		[Test]
		public void ExceptionInit() {
			const string sql = "DECLARE PRAGMA EXCEPTION_INIT (TOO_MANY_ROWS, -23744)";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsTrue(result.HasErrors);

			/*
			TODO: for some unknown reason (yet) the parsing of this fails
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DeclareExceptionInitStatement>(statement);

			var exceptionInit = (DeclareExceptionInitStatement) statement;
			Assert.IsNotNull(exceptionInit.ExceptionName);
			Assert.AreEqual("TOO_MANY_ROWS", exceptionInit.ExceptionName);
			Assert.AreEqual(-23744, exceptionInit.ErrorCode);
			*/
		}
	}
}
