using System;

using Deveel.Data.Transactions;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class LockTableStringFormatTests {
		[Test]
		public static void InSharedModeNoWait() {
			var statement = new LockTableStatement(ObjectName.Parse("APP.tab1"), LockingMode.Shared, 0);

			var sql = statement.ToString();
			const string expected = "LOCK TABLE APP.tab1 IN SHARED MODE NOWAIT";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void InExclusiveModeWait() {
			var statement = new LockTableStatement(ObjectName.Parse("APP.tab1"), LockingMode.Exclusive, 1200);

			var sql = statement.ToString();
			const string expected = "LOCK TABLE APP.tab1 IN EXCLUSIVE MODE WAIT 1200";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void InExclusiveModeInifiteWait() {
			var statement = new LockTableStatement(ObjectName.Parse("APP.tab1"), LockingMode.Exclusive);

			var sql = statement.ToString();
			const string expected = "LOCK TABLE APP.tab1 IN EXCLUSIVE MODE";

			Assert.AreEqual(expected, sql);
		}
	}
}
