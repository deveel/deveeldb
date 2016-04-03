using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Sequences;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropSequenceTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			var seqName = ObjectName.Parse("APP.seq1");
			Query.Access().CreateObject(new SequenceInfo(seqName, SqlNumber.Zero, SqlNumber.One, SqlNumber.Zero,
				new SqlNumber(Int64.MaxValue), Int32.MaxValue));
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
