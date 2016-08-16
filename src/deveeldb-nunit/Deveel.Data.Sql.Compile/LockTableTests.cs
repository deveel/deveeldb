using System;
using System.Linq;
using System.Threading;

using Deveel.Data.Sql.Statements;
using Deveel.Data.Transactions;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public class LockTableTests : SqlCompileTestBase {
		[Test]
		public void LockOneTable() {
			const string sql = "LOCK TABLE APP.tab1 IN SHARED MODE";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<LockTableStatement>(statement);

			var lockTable = (LockTableStatement) statement;

			Assert.IsNotNull(lockTable.TableName);
			Assert.AreEqual("APP.tab1", lockTable.TableName.FullName);
			Assert.AreEqual(LockingMode.Shared, lockTable.Mode);
			Assert.AreEqual(Timeout.Infinite, lockTable.WaitTimeout);
		}

		[Test]
		public void LockMultipleTables() {
			const string sql = "LOCK TABLE tab1, APP.tab2 IN EXCLUSIVE MODE";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(2, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<LockTableStatement>(statement);

			var lockTable = (LockTableStatement)statement;

			Assert.IsNotNull(lockTable.TableName);
			Assert.AreEqual("tab1", lockTable.TableName.FullName);
			Assert.AreEqual(LockingMode.Exclusive, lockTable.Mode);
			Assert.AreEqual(Timeout.Infinite, lockTable.WaitTimeout);
		}

		[Test]
		public void WaitLock() {
			const string sql = "LOCK TABLE APP.tab1 IN SHARED MODE WAIT 1000";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<LockTableStatement>(statement);

			var lockTable = (LockTableStatement)statement;

			Assert.IsNotNull(lockTable.TableName);
			Assert.AreEqual("APP.tab1", lockTable.TableName.FullName);
			Assert.AreEqual(LockingMode.Shared, lockTable.Mode);
			Assert.AreEqual(1000, lockTable.WaitTimeout);
		}

		[Test]
		public void LockNoWait() {
			const string sql = "LOCK TABLE APP.tab1 IN SHARED MODE NOWAIT";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<LockTableStatement>(statement);

			var lockTable = (LockTableStatement)statement;

			Assert.IsNotNull(lockTable.TableName);
			Assert.AreEqual("APP.tab1", lockTable.TableName.FullName);
			Assert.AreEqual(LockingMode.Shared, lockTable.Mode);
			Assert.AreEqual(0, lockTable.WaitTimeout);
		}
	}
}
