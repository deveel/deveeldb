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

using Deveel.Data.Configuration;
using Deveel.Data.DbSystem;
using Deveel.Data.Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class JoinTableTests : ContextBasedTest {
		private IUserSession session;
		private IQueryContext context;

		//[SetUp]
		//public void SetUp() {
		//	var systemContext = new SystemContext(DbConfig.Default);
		//	var dbContext = new DatabaseContext(systemContext, "testdb");
		//	var database = new Database(dbContext);
		//	database.Create("SA", "12345");
		//	database.Open();

		//	session = database.CreateSession(User.System);
		//	context = new SessionQueryContext(session);

		//	CreateTestTables();
		//	AddTestData();
		//}

		protected override void OnSetUp() {
			session = Database.CreateSession(AdminUserName, AdminPassword);
			context = new SessionQueryContext(session);

			CreateTestTables();
			AddTestData();
		}

		protected override void OnTearDown() {
			if (context != null)
				context.Dispose();
			if (session != null)
				session.Dispose();

			context = null;
			session = null;
		}

		private void AddTestData() {
			var table = session.GetMutableTable(new ObjectName(new ObjectName("APP"), "persons"));

			var row = table.NewRow();
			row["person_id"] = DataObject.Integer(1);
			row["name"] = DataObject.String("Antonello Provenzano");
			row["age"] = DataObject.Integer(34);
			table.AddRow(row);

			row = table.NewRow();
			row["person_id"] = DataObject.Integer(3);
			row["name"] = DataObject.String("John Doe");
			row["age"] = DataObject.Integer(56);
			table.AddRow(row);

			table = session.GetMutableTable(new ObjectName(new ObjectName("APP"), "codes"));
			row = table.NewRow();
			row["person_id"] = DataObject.Integer(1);
			row["code"] = DataObject.String("123456");
			row["registered"] = DataObject.Date(new SqlDateTime(2014, 01, 12));
			table.AddRow(row);
		}

		private void CreateTestTables() {
			var tableInfo = CreateFirstTable();
			session.CreateTable(tableInfo);
			session.AddPrimaryKey(tableInfo.TableName, "person_id");

			tableInfo = CreateSecondTable();
			session.CreateTable(tableInfo);
		}

		private TableInfo CreateSecondTable() {
			var tableInfo = new TableInfo(new ObjectName(new ObjectName("APP"), "codes"));
			tableInfo.AddColumn("person_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("code", PrimitiveTypes.String());
			tableInfo.AddColumn("registered", PrimitiveTypes.DateTime());

			return tableInfo;
		}

		private TableInfo CreateFirstTable() {
			var tableInfo = new TableInfo(new ObjectName(new ObjectName("APP"), "persons"));
			tableInfo.AddColumn("person_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("age", PrimitiveTypes.Integer());

			return tableInfo;
		}

		[TearDown]
		public void TearDown() {
		}

		[Test]
		public void NaturalInnerJoin() {
			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse("SELECT * FROM persons, codes"));
			Assert.IsNotNull(expression);
			Assert.IsInstanceOf<SqlQueryExpression>(expression);

			SqlExpression resultExpression = null;
			Assert.DoesNotThrow(() => resultExpression = expression.Evaluate(context, null));
			Assert.IsNotNull(resultExpression);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExpression);

			var constantExpression = (SqlConstantExpression) resultExpression;
			Assert.IsInstanceOf<QueryType>(constantExpression.Value.Type);

			SqlExpression tabularExpression = null;
			Assert.DoesNotThrow(() => tabularExpression = constantExpression.Evaluate(context, null));
			Assert.IsNotNull(tabularExpression);
			Assert.IsInstanceOf<SqlConstantExpression>(tabularExpression);

			constantExpression = (SqlConstantExpression) tabularExpression;
			var result = ((SqlTabular) constantExpression.Value.Value);

			Assert.IsNotNull(result);
		}
	}
}
