// 
//  Copyright 2010-2016 Deveel
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
//


using System;

using Deveel.Data.Routines;

namespace Deveel.Data.Sql.Triggers {
	public sealed class ProcedureTriggerInfo : TriggerInfo {
		public ProcedureTriggerInfo(ObjectName triggerName, ObjectName tableName, TriggerEventTime eventTime, TriggerEventType eventType, ObjectName procedureName) 
			: this(triggerName, tableName, eventTime, eventType, procedureName, new InvokeArgument[0]) {
		}

		public ProcedureTriggerInfo(ObjectName triggerName, ObjectName tableName, TriggerEventTime eventTime, TriggerEventType eventType, ObjectName procedureName, InvokeArgument[] args) 
			: base(triggerName, TriggerType.External, tableName, eventTime, eventType) {
			if (procedureName == null)
				throw new ArgumentNullException("procedureName");

			ProcedureName = procedureName;
			Arguments = args;
		}

		public ObjectName ProcedureName { get; private set; }

		public InvokeArgument[] Arguments { get; set; }

		public override TriggerInfo Rename(ObjectName name) {
			var args = Arguments == null ? new InvokeArgument[0] : new InvokeArgument[Arguments.Length];
			if (Arguments != null)
				Array.Copy(Arguments, args, Arguments.Length);

			return new ProcedureTriggerInfo(name, TableName, EventTime, EventType, ProcedureName, args);
		}
	}
}
