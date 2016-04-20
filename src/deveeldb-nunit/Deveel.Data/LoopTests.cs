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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class LoopTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			if (testName == "SimpleCursorForLoop")
				CreateTestTable();

			base.OnAfterSetup(testName);
		}

		private void CreateTestTable() {
			var tableName = ObjectName.Parse("APP.table1");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			Query.Access().CreateObject(tableInfo);

			var table = Query.Access().GetMutableTable(tableName);

			for (int i = 0; i < 50; i++) {
				var row = table.NewRow();
				row["a"] = Field.Integer(i);
				row["b"] = Field.String(String.Format("b_{0}", i));
				table.AddRow(row);
			}
		}

		[Test]
		public void LoopAndExitWithNoReturn() {
			var loop = new LoopStatement();
			loop.Statements.Add(new ReturnStatement(SqlExpression.Constant(45)));

			var result = Query.ExecuteStatement(loop);

			Assert.IsNotNull(result);
			Assert.AreEqual(StatementResultType.Result, result.Type);
		}

		[Test]
		public void SimpleForLoop() {
			var loop = new ForLoopStatement("i", SqlExpression.Constant(0), SqlExpression.Constant(200));
			loop.Statements.Add(new DeclareVariableStatement("a", PrimitiveTypes.String()));
			loop.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"),
				SqlExpression.FunctionCall("cast",
					new SqlExpression[] {SqlExpression.VariableReference("i"), SqlExpression.Constant("varchar")})));
			loop.Statements.Add(
				new ConditionStatement(SqlExpression.Equal(SqlExpression.VariableReference("i"), SqlExpression.Constant(200)),
					new SqlStatement[] {new ReturnStatement(SqlExpression.VariableReference("a"))}));

			 var result = Query.ExecuteStatement(loop);

			Assert.IsNotNull(result);
			Assert.AreEqual(StatementResultType.Result, result.Type);

			var value = result.Result.GetValue(0, 0);
			Assert.IsNotNull(value);
			Assert.IsFalse(Field.IsNullField(value));
			// TODO: the context should return the value of RETURN statement
		}

		[Test]
		public void SimpleCursorForLoop() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM table1");

			var block = new PlSqlBlockStatement();
			block.Declarations.Add(new DeclareCursorStatement("c1", query));
			
			var loop = new CursorForLoopStatement("i", "c1");
			loop.Statements.Add(new DeclareVariableStatement("a", PrimitiveTypes.String()));
			loop.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"),
				SqlExpression.FunctionCall("cast",
					new SqlExpression[] { SqlExpression.VariableReference("i"), SqlExpression.Constant("varchar") })));
			loop.Statements.Add(
				new ConditionStatement(SqlExpression.Equal(SqlExpression.VariableReference("i"), SqlExpression.Constant(50)),
					new SqlStatement[] { new ReturnStatement(SqlExpression.VariableReference("a")) }));
			block.Statements.Add(new OpenStatement("c1"));
			block.Statements.Add(loop);
			var result = Query.ExecuteStatement(block);

			Assert.IsNotNull(result);
			Assert.AreEqual(StatementResultType.Result, result.Type);

			var value = result.Result.GetValue(0, 0);
			Assert.IsNotNull(value);
			Assert.IsFalse(Field.IsNullField(value));
		}
	}
}
