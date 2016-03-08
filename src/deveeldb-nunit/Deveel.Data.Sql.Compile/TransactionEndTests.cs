using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class TransactionEndTests : SqlCompileTestBase {
		[Test]
		public void Commit() {
			const string sql = "COMMIT";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<CommitStatement>(statement);
		}

		[Test]
		public void Rollback() {
			const string sql = "ROLLBACK";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<RollbackStatement>(statement);
		}
	}
}