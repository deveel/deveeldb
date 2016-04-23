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

using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Triggers {
	[TestFixture]
	public sealed class TriggerListenTests : ContextBasedTest {
		private static readonly ObjectName TestTableName = ObjectName.Parse("APP.test_table");

		private TriggerEvent beforeEvent;
		private TriggerEvent afterEvent;

		private ISession testSession;
		private IQuery testQuery;

		protected override void OnAfterSetup(string testName) {
			testSession = Database.CreateUserSession(AdminUserName, AdminPassword);
			testQuery = testSession.CreateQuery();

			// TODO: this is a bug: since the session is new, the table should not exist
			if (testQuery.Access().TableExists(TestTableName))
				testQuery.Access().DropObject(DbObjectType.Table, TestTableName);

			var tableInfo = new TableInfo(TestTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer(), true);
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());

			testQuery.Access().CreateTable(tableInfo);

			if (!testName.EndsWith("_NoTriggers")) {
				testQuery.Access().CreateCallbackTrigger("callback1", TestTableName, TriggerEventTime.After, TriggerEventType.Insert);
			}

			System.Context.ListenTriggers(trigger => {
				if (trigger.EventTime == TriggerEventTime.After) {
					afterEvent = trigger;
				} else if (trigger.EventTime == TriggerEventTime.Before) {
					beforeEvent = trigger;
				}
			});
		}

		protected override void OnBeforeTearDown(string testName) {
			beforeEvent = null;
			afterEvent = null;

			if (testQuery != null)
				testQuery.Dispose();
			if (testSession != null) {
				testSession.Rollback();
				testSession.Dispose();
			}
		}

		[Test]
		public void Insert_NoTriggers() {
			var table = testQuery.Access().GetMutableTable(TestTableName);

			Assert.IsNotNull(table);

			var row = table.NewRow();
			row.SetValue(0, 1);
			row.SetValue(1, "Antonello");
			row.SetValue(2, "Provenzano");

			table.AddRow(row);
			testQuery.Session.Commit();

			Assert.IsNull(beforeEvent);
			Assert.IsNull(afterEvent);
		}

		[Test]
		public void Insert() {
			var table = testQuery.Access().GetMutableTable(TestTableName);

			Assert.IsNotNull(table);

			var row = table.NewRow();
			row.SetValue(0, 1);
			row.SetValue(1, "Antonello");
			row.SetValue(2, "Provenzano");

			table.AddRow(row);
			testQuery.Session.Commit();

			Assert.IsNull(beforeEvent);
			Assert.IsNotNull(afterEvent);
		}
	}
}
