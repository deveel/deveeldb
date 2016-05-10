// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Linq;

using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

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


		[Test]
		public void AddMultipleColumns() {
			const string sql = "ALTER TABLE test ADD COLUMN b INT NOT NULL ADD c VARCHAR DEFAULT 'test'";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(2, result.Statements.Count);
		}

		[Test]
		public void AddColumnsAndUniqeContraints() {
			const string sql = "ALTER TABLE test ADD COLUMN b INT NOT NULL ADD CONSTRAINT c UNIQUE(a, b)";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(2, result.Statements.Count);

			var firstStatement = result.Statements.ElementAt(0);
			var secondStatement = result.Statements.ElementAt(1);

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
			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.First();

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
			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<AlterTableStatement>(statement);

			var alter = (AlterTableStatement) statement;

			Assert.IsInstanceOf<DropColumnAction>(alter.Action);
		}

		[Test]
		public void DropConstraint() {
			const string sql = "ALTER TABLE test DROP CONSTRAINT test_UNIQUE_IX";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<AlterTableStatement>(statement);

			var alter = (AlterTableStatement)statement;

			Assert.IsInstanceOf<DropConstraintAction>(alter.Action);
		}

		[Test]
		public void DropPrimaryKey() {
			const string sql = "ALTER TABLE test DROP PRIMARY KEY";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<AlterTableStatement>(statement);

			var alter = (AlterTableStatement)statement;

			Assert.IsInstanceOf<DropPrimaryKeyAction>(alter.Action);
		}

		[Test]
		public void SetDefault() {
			const string sql = "ALTER TABLE test ALTER col1 SET DEFAULT 'one test'";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<AlterTableStatement>(statement);

			var alter = (AlterTableStatement)statement;

			Assert.IsInstanceOf<SetDefaultAction>(alter.Action);

			var action = (SetDefaultAction) alter.Action;
			Assert.IsNotNull(action);
			Assert.IsNotNull(action.DefaultExpression);
		}


		[Test]
		public void DropDefault() {
			const string sql = "ALTER TABLE test ALTER col1 DROP DEFAULT";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<AlterTableStatement>(statement);

			var alter = (AlterTableStatement)statement;

			Assert.IsInstanceOf<DropDefaultAction>(alter.Action);
		}
	}
}
