using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateSequenceTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			var seqName = ObjectName.Parse("APP.seq1");
			Query.Access().CreateObject(new SequenceInfo(seqName, SqlNumber.Zero, SqlNumber.One, SqlNumber.Zero,
				new SqlNumber(Int64.MaxValue), Int32.MaxValue));
		}

		[Test]
		public void Simple() {
			var seqName = ObjectName.Parse("APP.seq2");

			Query.CreateSequence(seqName);

			var exists = Query.Access().ObjectExists(DbObjectType.Sequence, seqName);
			Assert.IsTrue(exists);
		}

		[Test]
		public void WithValues() {
			var seqName = ObjectName.Parse("APP.seq2");
			Query.CreateSequence(seqName,
				SqlExpression.Constant(0),
				SqlExpression.Constant(1),
				SqlExpression.Constant(0),
				SqlExpression.Constant(Int64.MaxValue),
				SqlExpression.Constant(Int16.MaxValue));

			var exists = Query.Access().ObjectExists(DbObjectType.Sequence, seqName);
			Assert.IsTrue(exists);
		}

		[Test]
		public void Existing_UserDefined() {
			var seqName = ObjectName.Parse("APP.seq1");

			Assert.Throws<StatementException>(() => Query.CreateSequence(seqName));
		}

		[Test]
		public void Existing_Native() {
			var tableName = SystemSchema.TableInfoTableName;

			Assert.Throws<StatementException>(() => Query.CreateSequence(tableName));
		}
	}
}
