using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Sequences;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropSequenceTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var seqName = ObjectName.Parse("APP.seq1");
			query.Access().CreateObject(new SequenceInfo(seqName, SqlNumber.Zero, SqlNumber.One, SqlNumber.Zero,
				new SqlNumber(Int64.MaxValue), Int32.MaxValue));
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			return true;
		}

		[Test]
		public void Existing() {
			var seqName = ObjectName.Parse("APP.seq1");

			Query.DropSequence(seqName);

			var exists = Query.Access().ObjectExists(DbObjectType.Sequence, seqName);
			Assert.IsFalse(exists);
		}
	}
}
