using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SequenceFunctionTests : FunctionTestBase {
		protected override void OnSetUp(string testName) {
			var info = new SequenceInfo(ObjectName.Parse("APP.seq1"),
				new SqlNumber(0), new SqlNumber(1), new SqlNumber(0), new SqlNumber(Int64.MaxValue), false);

			Query.Access().CreateObject(info);
		}

		protected override void OnTearDown() {
			Query.Access().DropObject(DbObjectType.Sequence, ObjectName.Parse("APP.seq1"));
		}

		[Test]
		public void NextValue() {
			var result = Select("NEXTVAL", SqlExpression.Constant("APP.seq1"));

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var value = (SqlNumber) result.Value;
			Assert.AreEqual(new SqlNumber(1), value);
		}

		[Test]
		public void CurrentValue() {
			var result = Select("CURVAL", SqlExpression.Constant("APP.seq1"));

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var value = (SqlNumber)result.Value;
			Assert.AreEqual(new SqlNumber(0), value);
		}
	}
}
