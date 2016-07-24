using System;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class TransactionEndStringFormatTests {
		[Test]
		public static void Commit() {
			var statement = new CommitStatement();

			var sql = statement.ToString();

			Assert.AreEqual("COMMIT", sql);
		}

		[Test]
		public static void SimpleRollback() {
			var statement = new RollbackStatement();

			var sql = statement.ToString();

			Assert.AreEqual("ROLLBACK", sql);
		}
	}
}
