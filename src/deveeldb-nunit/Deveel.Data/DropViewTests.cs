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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropViewTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTestView(query);
			return true;
		}

		private static void CreateTestView(IQuery query) {
			var tn1 = ObjectName.Parse("APP.test_table1");
			query.Access().CreateTable(table => table
				.Named(tn1)
				.WithColumn("id", PrimitiveTypes.Integer())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("date", PrimitiveTypes.DateTime()));

			query.Session.Access().AddPrimaryKey(tn1, "id");

			var exp = SqlExpression.Parse("SELECT * FROM APP.test_table1");
			query.CreateView(ObjectName.Parse("APP.test_view1"), (SqlQueryExpression)exp);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tn1 = ObjectName.Parse("APP.test_table1");
			var viewName = ObjectName.Parse("APP.test_view1");

			query.Access().DropAllTableConstraints(tn1);
			query.Access().DropObject(DbObjectType.View, viewName);
			query.Access().DropObject(DbObjectType.Table, tn1);
			return true;
		}

		[Test]
		public void DropView() {
			var viewName = ObjectName.Parse("APP.test_view1");

			AdminQuery.DropView(viewName);

			var exists = AdminQuery.Session.Access().ViewExists(viewName);
			Assert.IsFalse(exists);
		}

		[Test]
		public void IfExists_Existing() {
			var viewName = ObjectName.Parse("APP.test_view1");

			AdminQuery.DropView(viewName, true);

			var exists = AdminQuery.Session.Access().ViewExists(viewName);
			Assert.IsFalse(exists);
		}

		[Test]
		public void IfExists_NotExisting() {
			var viewName = ObjectName.Parse("APP.test_view2");

			AdminQuery.DropView(viewName, true);

			var exists = AdminQuery.Session.Access().ViewExists(viewName);
			Assert.IsFalse(exists);
		}

	}
}
