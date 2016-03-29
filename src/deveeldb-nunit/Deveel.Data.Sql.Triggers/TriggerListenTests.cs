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

		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);

			var tableInfo = new TableInfo(TestTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer(), true);
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());

			query.Session.Access.CreateTable(tableInfo);

			return query;
		}

		private TriggerEvent beforeEvent;
		private TriggerEvent afterEvent;

		protected override ISystem CreateSystem() {
			var system = base.CreateSystem();
			system.Context.ListenTriggers(trigger => {
				if ((trigger.TriggerEventType & TriggerEventType.After) != 0) {
					afterEvent = trigger;
				} else if ((trigger.TriggerEventType & TriggerEventType.Before) != 0) {
					beforeEvent = trigger;
				}
			});

			return system;
		}

		protected override void OnSetUp(string testName) {
			if (!testName.EndsWith("_NoTriggers")) {
				Query.Access.CreateCallbackTrigger("callback1", TestTableName, TriggerEventType.AfterInsert);
			}

			base.OnSetUp(testName);
		}

		[Test]
		public void Insert_NoTriggers() {
			var table = Query.Access.GetMutableTable(TestTableName);

			Assert.IsNotNull(table);

			var row = table.NewRow();
			row.SetValue(0, 1);
			row.SetValue(1, "Antonello");
			row.SetValue(2, "Provenzano");

			Assert.DoesNotThrow(() => table.AddRow(row));
			Assert.DoesNotThrow(() => Query.Session.Commit());

			Assert.IsNull(beforeEvent);
			Assert.IsNull(afterEvent);
		}

		[Test]
		public void Insert() {
			var table = Query.Access.GetMutableTable(TestTableName);

			Assert.IsNotNull(table);

			var row = table.NewRow();
			row.SetValue(0, 1);
			row.SetValue(1, "Antonello");
			row.SetValue(2, "Provenzano");

			table.AddRow(row);
			Query.Session.Commit();

			Assert.IsNull(beforeEvent);
			Assert.IsNotNull(afterEvent);
		}
	}
}
