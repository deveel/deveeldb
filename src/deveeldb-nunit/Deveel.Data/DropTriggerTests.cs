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
using Deveel.Data.Sql.Triggers;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropTriggerTests : ContextBasedTest {
		private ObjectName triggerName;

		protected override bool OnSetUp(string testName, IQuery query) {
			triggerName = ObjectName.Parse("APP.trigger1");
			query.Access().CreateTrigger(new ProcedureTriggerInfo(triggerName, ObjectName.Parse("APP.table1"),
				TriggerEventTime.After, TriggerEventType.Delete, ObjectName.Parse("APP.proc1")));

			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DropTrigger(triggerName);
			return true;
		}

		[Test]
		public void ExistingTrigger() {
			Query.DropTrigger(triggerName);

			var exists = Query.Access().TriggerExists(triggerName);

			Assert.IsFalse(exists);
		}
	}
}
