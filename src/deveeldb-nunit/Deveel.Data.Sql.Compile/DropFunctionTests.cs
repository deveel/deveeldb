using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class DropFunctionTests : SqlCompileTestBase {
		[Test]
		public void SimpleDrop() {
			const string sql = "DROP FUNCTION func1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);
			Assert.IsInstanceOf<DropFunctionStatement>(result.Statements.ElementAt(0));

			var dropFunction = (DropFunctionStatement)result.Statements.ElementAt(0);

			Assert.IsNotNull(dropFunction);
			Assert.IsNotNull(dropFunction.FunctionName);
			Assert.AreEqual("func1", dropFunction.FunctionName.FullName);
			Assert.IsFalse(dropFunction.IfExists);
		}

		[Test]
		public void DropIfExists() {
			const string sql = "DROP FUNCTION IF EXISTS sys.func1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);
			Assert.IsInstanceOf<DropFunctionStatement>(result.Statements.ElementAt(0));

			var dropProcedure = (DropFunctionStatement)result.Statements.ElementAt(0);

			Assert.IsNotNull(dropProcedure);
			Assert.IsNotNull(dropProcedure.FunctionName);
			Assert.AreEqual("sys.func1", dropProcedure.FunctionName.FullName);
			Assert.IsTrue(dropProcedure.IfExists);
		}
	}
}
