using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class DropProcedureTests : SqlCompileTestBase {
		[Test]
		public void SimpleDrop() {
			const string sql = "DROP PROCEDURE proc1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);
			Assert.IsInstanceOf<DropProcedureStatement>(result.Statements.ElementAt(0));

			var dropProcedure = (DropProcedureStatement) result.Statements.ElementAt(0);

			Assert.IsNotNull(dropProcedure);
			Assert.IsNotNull(dropProcedure.ProcedureName);
			Assert.AreEqual("proc1", dropProcedure.ProcedureName.FullName);
			Assert.IsFalse(dropProcedure.IfExists);
		}

		[Test]
		public void DropIfExists() {
			const string sql = "DROP PROCEDURE IF EXISTS sys.proc1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);
			Assert.IsInstanceOf<DropProcedureStatement>(result.Statements.ElementAt(0));

			var dropProcedure = (DropProcedureStatement)result.Statements.ElementAt(0);

			Assert.IsNotNull(dropProcedure);
			Assert.IsNotNull(dropProcedure.ProcedureName);
			Assert.AreEqual("sys.proc1", dropProcedure.ProcedureName.FullName);
			Assert.IsTrue(dropProcedure.IfExists);
		}
	}
}
