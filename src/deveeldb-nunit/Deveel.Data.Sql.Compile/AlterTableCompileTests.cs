using System;
using System.Linq;

using Deveel.Data.Sql.Statements;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public class AlterTableCompileTests : CursorCompileTests {
		[Test]
		public void AlterTableAddColumn() {
			const string sql = "ALTER TABLE test ADD COLUMN b INT NOT NULL";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.First();

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
	}
}
