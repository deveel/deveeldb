using System;

using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class CreateSequenceStatementTests : ContextBasedTest {
		[Test]
		public void CreateWithDefaultValues() {
			var seqName = ObjectName.Parse("APP.test_seq1");
			var statement = new CreateSequenceStatement(seqName);

			statement.Execute(Query);

			var exists = Query.ObjectExists(DbObjectType.Sequence, seqName);

			Assert.IsTrue(exists);
		}

		[Test]
		public void CreateWithStartValue() {
			var seqName = ObjectName.Parse("APP.test_seq1");
			var statement = new CreateSequenceStatement(seqName) {
				StartWith = SqlExpression.Constant(2)
			};

			statement.Execute(Query);

			var exists = Query.ObjectExists(DbObjectType.Sequence, seqName);

			Assert.IsTrue(exists);
		}
	}
}
