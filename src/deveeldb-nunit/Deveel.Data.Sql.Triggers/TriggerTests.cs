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

using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Triggers {
	[TestFixture]
	public class TriggerTests : ContextBasedTest {
		private static readonly ObjectName TestTableName = ObjectName.Parse("APP.test_table");

		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			var tableInfo = new TableInfo(TestTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("date", PrimitiveTypes.DateTime());
			query.CreateTable(tableInfo);
			return query;
		}

		[Test]
		public void CreateCallbackTrigger() {
			var triggerName = ObjectName.Parse("APP.test_trigger");
			Assert.DoesNotThrow(() => Query.CreateCallbackTrigger(triggerName, TriggerEventType.BeforeInsert));

			bool exists = false;
			Assert.DoesNotThrow(() => exists = Query.TriggerExists(triggerName));
			Assert.IsTrue(exists);
		}

		[Test]
		public void CreateProcedureTrigger() {
			
		}
	}
}
