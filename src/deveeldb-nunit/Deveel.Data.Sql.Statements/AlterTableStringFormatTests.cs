using System;
using System.Text;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class AlterTableStringFormatTests {
		[Test]
		public static void AddColumn() {
			var column = new SqlTableColumn("a", PrimitiveTypes.Integer());
			var statement = new AlterTableStatement(ObjectName.Parse("APP.test_table"), new AddColumnAction(column));

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.Append("ALTER TABLE APP.test_table ");
			expected.Append("ADD COLUMN a INTEGER");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void DropColumn() {
			var statement = new AlterTableStatement(ObjectName.Parse("APP.test"), new DropColumnAction("a"));

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.Append("ALTER TABLE APP.test DROP COLUMN a");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void AddPrimaryKey() {
			var constraint = new SqlTableConstraint(ConstraintType.PrimaryKey, new[] { "id" });
			var statement = new AlterTableStatement(ObjectName.Parse("test"), new AddConstraintAction(constraint));

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.Append("ALTER TABLE test ADD CONSTRAINT PRIMARY KEY(id)");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void SimpleAddForeignKey() {
			var constraint = new SqlTableConstraint(ConstraintType.ForeignKey, new[] { "ref_id" }) {
				ReferenceTable = "test_table1",
				ReferenceColumns = new[] { "id" }
			};

			var statement = new AlterTableStatement(ObjectName.Parse("test"), new AddConstraintAction(constraint));

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.Append("ALTER TABLE test ADD CONSTRAINT FOREIGN KEY(ref_id) ");
			expected.Append("REFERENCES test_table1(id) ");
			expected.Append("ON DELETE NO ACTION ");
			expected.Append("ON UPDATE NO ACTION");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void AddCheck() {
			var constraint = new SqlTableConstraint(ConstraintType.Check) {
				CheckExpression = SqlExpression.Equal(SqlExpression.Reference(new ObjectName("a")),
					SqlExpression.Constant(2))
			};

			var statement = new AlterTableStatement(ObjectName.Parse("test"), new AddConstraintAction(constraint));

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.Append("ALTER TABLE test ADD CONSTRAINT ");
			expected.Append("CHECK a = 2");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void AddUnique() {
			var constraint = new SqlTableConstraint("unique_id", ConstraintType.Unique, new[] { "id" });
			var statement = new AlterTableStatement(ObjectName.Parse("test"), new AddConstraintAction(constraint));

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.Append("ALTER TABLE test ADD CONSTRAINT unique_id UNIQUE(id)");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void DropPrimaryKey() {
			var statement = new AlterTableStatement(ObjectName.Parse("APP.test"), new DropPrimaryKeyAction());

			var sql = statement.ToString();
			var expected = "ALTER TABLE APP.test DROP PRIMARY KEY";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void DropConstraint() {
			var statement = new AlterTableStatement(ObjectName.Parse("APP.test"), new DropConstraintAction("unique_id"));

			var sql = statement.ToString();
			var expected = "ALTER TABLE APP.test DROP CONSTRAINT unique_id";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void DropDefault() {
			var statement = new AlterTableStatement(ObjectName.Parse("test"), new DropDefaultAction("a"));

			var sql = statement.ToString();
			var expected = "ALTER TABLE test DROP DEFAULT a";

			Assert.AreEqual(expected, sql);
		}
	}
}
