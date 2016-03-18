using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateSequenceTests : ContextBasedTest {
		[Test]
		public void Simple() {
			var seqName = ObjectName.Parse("APP.seq1");

			Query.CreateSequence(seqName);

			var exists = Query.Access.ObjectExists(DbObjectType.Sequence, seqName);
			Assert.IsTrue(exists);
		}

		[Test]
		public void WithValues() {
			var seqName = ObjectName.Parse("APP.seq1");
			Query.CreateSequence(seqName,
				SqlExpression.Constant(0),
				SqlExpression.Constant(1),
				SqlExpression.Constant(0),
				SqlExpression.Constant(Int64.MaxValue),
				SqlExpression.Constant(Int16.MaxValue));

			var exists = Query.Access.ObjectExists(DbObjectType.Sequence, seqName);
			Assert.IsTrue(exists);
		}
	}
}
