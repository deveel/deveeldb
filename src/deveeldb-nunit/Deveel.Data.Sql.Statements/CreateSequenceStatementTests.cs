using System;

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
	}
}
