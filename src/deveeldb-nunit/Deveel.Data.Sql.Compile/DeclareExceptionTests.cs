using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class DeclareExceptionTests : SqlCompileTestBase {
		[Test]
		public void SimpleDeclare() {
			const string sql = "DECLARE TestEx EXCEPTION";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DeclareExceptionStatement>(statement);

			var declareException = (DeclareExceptionStatement) statement;

			Assert.AreEqual("TestEx", declareException.ExceptionName);
		}
	}
}