using System;
using System.Linq;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SelectSequenceTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var seqName = ObjectName.Parse("APP.seq1");
			query.Access().CreateObject(new SequenceInfo(seqName, new SqlNumber(0), new SqlNumber(1), new SqlNumber(0),
				new SqlNumber(Int16.MaxValue), 256));
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var seqName = ObjectName.Parse("APP.seq1");
			query.Access().DropObject(DbObjectType.Sequence, seqName);
			return true;
		}

		private Field SelectScalar(string column) {
			var query = new SqlQueryExpression(new [] {new SelectColumn(SqlExpression.Reference(new ObjectName(column))) });
			query.FromClause.AddTable("seq1");

			var result = Query.Select(query);
			var row = result.FirstOrDefault();
			if (row == null)
				return Field.Null();

			return row.GetValue(0);
		}

		[Test]
		public void CurrentValue() {
			var value = SelectScalar("current_value");

			Assert.IsNotNull(value);
			Assert.IsFalse(Field.IsNullField(value));

			Assert.IsInstanceOf<NumericType>(value.Type);
		}

		[Test]
		public void IncrementBy() {
			var value = SelectScalar("increment_by");

			Assert.IsNotNull(value);
			Assert.IsFalse(Field.IsNullField(value));

			Assert.IsInstanceOf<NumericType>(value.Type);
		}
	}
}
