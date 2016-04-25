using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CallTests : SqlCompileTestBase {
		[Test]
		public void WithoutArguments() {
			const string sql = "CALL func1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CallStatement>(statement);

			var call = (CallStatement) statement;

			Assert.IsNotNull(call.ProcedureName);
			Assert.AreEqual("func1", call.ProcedureName.FullName);
			Assert.IsNotNull(call.Arguments);
			Assert.IsEmpty(call.Arguments);
		}

		[Test]
		public void WithAnonymousArguments() {
			const string sql = "CALL sys.func1('Hello', 'World')";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CallStatement>(statement);

			var call = (CallStatement)statement;

			Assert.IsNotNull(call.ProcedureName);
			Assert.AreEqual("sys.func1", call.ProcedureName.FullName);
			Assert.IsNotNull(call.Arguments);
			Assert.IsNotEmpty(call.Arguments);
			Assert.AreEqual(2, call.Arguments.Length);
		}

		[Test]
		public void WithNamedArguments() {
			const string sql = "CALL func1(a => 'Hello', b => 'World')";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CallStatement>(statement);

			var call = (CallStatement)statement;

			Assert.IsNotNull(call.ProcedureName);
			Assert.AreEqual("func1", call.ProcedureName.FullName);
			Assert.IsNotNull(call.Arguments);
			Assert.IsNotEmpty(call.Arguments);
		}

		[Test]
		public void WithMxedNamedAndAnonymousArguments() {
			const string sql = "CALL func1(a => 'Hello', 'World')";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsTrue(result.HasErrors);
		}
	}
}
