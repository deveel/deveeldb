using System;
using System.Linq;

using Deveel.Data.Sql.Statements;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public class AlterTableCompileTests : SqlCompileTestBase {
		[Test]
		public void AddColumn() {
			const string sql = "ALTER TABLE test ADD COLUMN b INT NOT NULL";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.IsNotEmpty(result.CodeObjects);
			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.First();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<AlterTableStatement>(statement);

			var alterStatement = (AlterTableStatement) statement;
			
			Assert.AreEqual("test", alterStatement.TableName.FullName);
			Assert.IsInstanceOf<AddColumnAction>(alterStatement.Action);

			var alterAction = (AddColumnAction) alterStatement.Action;
			Assert.AreEqual("b", alterAction.Column.ColumnName);
			Assert.IsInstanceOf<NumericType>(alterAction.Column.ColumnType);
			Assert.IsTrue(alterAction.Column.IsNotNull);
		}


		[Test]
		public void AddMultipleColumns() {
			const string sql = "ALTER TABLE test ADD COLUMN b INT NOT NULL ADD c VARCHAR DEFAULT 'test'";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.IsNotEmpty(result.CodeObjects);
			Assert.AreEqual(2, result.CodeObjects.Count);
		}

		[Test]
		public void AddColumnsAndUniqeContraints() {
			const string sql = "ALTER TABLE test ADD COLUMN b INT NOT NULL ADD CONSTRAINT c UNIQUE(a, b)";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.IsNotEmpty(result.CodeObjects);
			Assert.AreEqual(2, result.CodeObjects.Count);

			var firstStatement = result.CodeObjects.ElementAt(0);
			var secondStatement = result.CodeObjects.ElementAt(1);

			Assert.IsNotNull(firstStatement);
			Assert.IsNotNull(secondStatement);

			Assert.IsInstanceOf<AlterTableStatement>(firstStatement);
			Assert.IsInstanceOf<AlterTableStatement>(secondStatement);

			var alter1 = (AlterTableStatement) firstStatement;
			var alter2 = (AlterTableStatement) secondStatement;

			Assert.IsInstanceOf<AddColumnAction>(alter1.Action);
			Assert.IsInstanceOf<AddConstraintAction>(alter2.Action);
		}

		[Test]
		public void AddPrimaryKeyContraint() {
			const string sql = "ALTER TABLE test ADD CONSTRAINT c PRIMARY KEY(id)";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.IsNotEmpty(result.CodeObjects);
			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.First();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<AlterTableStatement>(statement);

			var alterStatement = (AlterTableStatement) statement;
			
			Assert.AreEqual("test", alterStatement.TableName.FullName);
			Assert.IsInstanceOf<AddConstraintAction>(alterStatement.Action);			
		}

		[Test]
		public void DropColumn() {
			const string sql = "ALTER TABLE test DROP COLUMN b";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.IsNotEmpty(result.CodeObjects);
			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.ElementAt(0);

			Assert.IsInstanceOf<AlterTableStatement>(statement);

			var alter = (AlterTableStatement) statement;

			Assert.IsInstanceOf<DropColumnAction>(alter.Action);
		}
	}
}
