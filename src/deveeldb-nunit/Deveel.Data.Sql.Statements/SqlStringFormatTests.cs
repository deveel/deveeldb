using System;
using System.Text;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class SqlStringFormatTests {
		[Test]
		public static void CreateTable() {
			var statement = new CreateTableStatement(new ObjectName("test_table"),
				new[] {
					new SqlTableColumn("a", PrimitiveTypes.Integer()),
					new SqlTableColumn("b", PrimitiveTypes.String()), 
				});

			var sql = statement.ToString();

			var expected = new StringBuilder();
			expected.AppendLine("CREATE TABLE test_table (");
			expected.AppendLine("  a INTEGER,");
			expected.AppendLine("  b STRING");
			expected.Append(")");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void AlterTable_AddColumn() {
			var column = new SqlTableColumn("a", PrimitiveTypes.Integer());
			var statement = new AlterTableStatement(ObjectName.Parse("APP.test_table"), new AddColumnAction(column));

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.Append("ALTER TABLE APP.test_table ");
			expected.Append("ADD COLUMN a INTEGER");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void AlterTable_DropColumn() {
			var statement = new AlterTableStatement(ObjectName.Parse("APP.test"), new DropColumnAction("a"));

			var sql = statement.ToString();
			var expected=new StringBuilder();
			expected.Append("ALTER TABLE APP.test DROP COLUMN a");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void AlterTable_AddPrimaryKey() {
			var constraint = new SqlTableConstraint(ConstraintType.PrimaryKey, new[] {"id"});
			var statement = new AlterTableStatement(ObjectName.Parse("test"), new AddConstraintAction(constraint));

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.Append("ALTER TABLE test ADD CONSTRAINT PRIMARY KEY(id)");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void AlterTable_SimpleAddForeignKey() {
			var constraint = new SqlTableConstraint(ConstraintType.ForeignKey, new[] { "ref_id" }) {
				ReferenceTable = "test_table1",
				ReferenceColumns = new []{"id"}
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
		public static void AlterTable_AddCheck() {
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
		public static void AlterTable_AddUnique() {
			var constraint = new SqlTableConstraint("unique_id", ConstraintType.Unique, new []{"id"});
			var statement = new AlterTableStatement(ObjectName.Parse("test"), new AddConstraintAction(constraint));

			var sql = statement.ToString();
			var expected = new StringBuilder();
			expected.Append("ALTER TABLE test ADD CONSTRAINT unique_id UNIQUE(id)");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void AlterTable_DropPrimaryKey() {
			var statement = new AlterTableStatement(ObjectName.Parse("APP.test"), new DropPrimaryKeyAction());

			var sql = statement.ToString();
			var expected = "ALTER TABLE APP.test DROP PRIMARY KEY";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void AlterTable_DropConstraint() {
			var statement = new AlterTableStatement(ObjectName.Parse("APP.test"), new DropConstraintAction("unique_id"));

			var sql = statement.ToString();
			var expected = "ALTER TABLE APP.test DROP CONSTRAINT unique_id";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void AlterTable_DropDefault() {
			var statement = new AlterTableStatement(ObjectName.Parse("test"), new DropDefaultAction("a"));

			var sql = statement.ToString();
			var expected = "ALTER TABLE test DROP DEFAULT a";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void CodeBlock_Nolabel() {
			var block = new PlSqlBlockStatement();
			block.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"), SqlExpression.Constant(2)));

			var sql = block.ToString();
			var expected = new StringBuilder();
			expected.AppendLine("BEGIN");
			expected.AppendLine("  :a := 2");
			expected.Append("END");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void CodeBlock_WithLabel() {
			var block = new PlSqlBlockStatement();
			block.Label = "stmt";
			block.Statements.Add(new CallStatement(ObjectName.Parse("proc1"), new[] {
				SqlExpression.Constant(33)
			}));

			var sql = block.ToString();
			var expected = new StringBuilder();
			expected.AppendLine("<<stmt>>");
			expected.AppendLine("BEGIN");
			expected.AppendLine("  CALL proc1(33)");
			expected.Append("END");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void Loop_NoLabel() {
			var loop = new LoopStatement();
			loop.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"), SqlExpression.Constant("two")));
			loop.Statements.Add(new ConditionStatement(SqlExpression.Constant(true), new SqlStatement[] {
				new ExitStatement() 
			}));

			var sql = loop.ToString();
			var expected = new StringBuilder();
			expected.AppendLine("LOOP");
			expected.AppendLine("  :a := 'two'");
			expected.AppendLine("  IF true THEN");
			expected.AppendLine("    EXIT");
			expected.AppendLine("  END IF");
			expected.Append("END LOOP");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void Call_NoArgs() {
			var statement = new CallStatement(ObjectName.Parse("proc1"));

			var sql = statement.ToString();

			Assert.AreEqual("CALL proc1()", sql);
		}

		[Test]
		public static void Call_AnonArgs() {
			var statement = new CallStatement(ObjectName.Parse("proc1"), new SqlExpression[] {
				SqlExpression.Constant("one")
			});

			var sql = statement.ToString();

			Assert.AreEqual("CALL proc1('one')", sql);
		}

		[Test]
		public static void Call_NamedArgs() {
			var statement = new CallStatement(ObjectName.Parse("APP.proc1"), new InvokeArgument[] {
				new InvokeArgument("a", SqlExpression.Constant(Field.Number(new SqlNumber(8399.22, 6)))) 
			});

			var sql = statement.ToString();

			Assert.AreEqual("CALL APP.proc1(a => 8399.22)", sql);
		}
	}
}
