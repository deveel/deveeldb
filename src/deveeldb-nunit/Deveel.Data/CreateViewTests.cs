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
	public sealed class CreateViewTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			query.Access().CreateTable(table => table
				.Named("APP.test_table")
				.WithColumn("a", PrimitiveTypes.Integer())
				.WithColumn("b", PrimitiveTypes.String()));
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var viewName = ObjectName.Parse("APP.text_view1");
			query.Access().DropObject(DbObjectType.View, viewName);

			query.Access().DropObject(DbObjectType.Table, ObjectName.Parse("APP.test_table"));
			return true;
		}

		[Test]
		public void SimpleView() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM test_table WHERE a = 1");
			var viewName = ObjectName.Parse("APP.text_view1");

			AdminQuery.CreateView(viewName, query);

			// TODO: Assert the view exists
		}

		[Test]
		public void ViewFromBuilder() {
			var viewName = ObjectName.Parse("APP.text_view1");

			AdminQuery.CreateView(viewName, query => query
				.AllColumns()
				.From(from => from.Table("test_table"))
				.Where(SqlExpression.Equal(SqlExpression.Reference(new ObjectName("a")), SqlExpression.Constant(1))));
		}
	}
}
