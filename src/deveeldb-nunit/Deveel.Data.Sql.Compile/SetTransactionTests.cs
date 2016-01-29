using System;
using System.Linq;

using Deveel.Data.Sql.Statements;
using Deveel.Data.Transactions;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class SetTransactionTests : SqlCompileTestBase {
		[TestCase("READ COMMITTED", IsolationLevel.ReadCommitted)]
		[TestCase("READ UNCOMMITTED", IsolationLevel.ReadUncommitted)]
		[TestCase("SERIALIZABLE", IsolationLevel.Serializable)]
		public void SetIsolationLevel(string typePart, IsolationLevel expected) {
			var sql = String.Format("SET TRANSACTION ISOLATION LEVEL {0}", typePart);

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SetIsolationLevelStatement>(statement);

			var setIsolationLevel = (SetIsolationLevelStatement) statement;

			Assert.AreEqual(expected, setIsolationLevel.IsolationLevel);
		}

		[TestCase("READ ONLY", true)]
		[TestCase("READ WRITE", false)]
		public void SetReadOnly(string accessType, bool expectedStatus) {
			var sql = String.Format("SET TRANSACTION {0}", accessType);

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SetReadOnlyStatement>(statement);

			var setReadOnly = (SetReadOnlyStatement)statement;

			Assert.AreEqual(expectedStatus, setReadOnly.Status);
		}
	}
}
