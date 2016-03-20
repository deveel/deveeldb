using System;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;
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
			Assert.IsInstanceOf<SetStatement>(statement);

			var set = (SetStatement)statement;
			Assert.AreEqual(TransactionSettingKeys.IsolationLevel, set.SettingName);
			Assert.IsInstanceOf<SqlConstantExpression>(set.ValueExpression);

			var value = (SqlConstantExpression)set.ValueExpression;
			var field = value.Value;

			Assert.IsInstanceOf<StringType>(field.Type);

			var s = (SqlString) field.Value;

			Assert.AreEqual(expected.ToString(), s.ToString());
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
			Assert.IsInstanceOf<SetStatement>(statement);

			var set = (SetStatement)statement;
			Assert.AreEqual(TransactionSettingKeys.ReadOnly, set.SettingName);
			Assert.IsInstanceOf<SqlConstantExpression>(set.ValueExpression);

			var value = (SqlConstantExpression) set.ValueExpression;
			var field = value.Value;

			Assert.IsInstanceOf<BooleanType>(field.Type);

			var status = (bool) ((SqlBoolean) field.Value);
			Assert.AreEqual(expectedStatus, status);
		}
	}
}
