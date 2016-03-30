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
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropCallbackTriggersStatement : SqlStatement {
		public DropCallbackTriggersStatement(string triggerName) {
			if (String.IsNullOrEmpty(triggerName))
				throw new ArgumentNullException("triggerName");

			TriggerName = triggerName;
		}

		private DropCallbackTriggersStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			TriggerName = info.GetString("TriggerName");
		}

		public string TriggerName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.DirectAccess.TriggerExists(new ObjectName(TriggerName)))
				throw new StatementException(String.Format("The callback trigger '{0}' does not exist in the context.", TriggerName));

			if (!context.DirectAccess.DropCallbackTrigger(TriggerName))
				throw new StatementException(String.Format("Could not drop the callback trigger '{0}' from the context", TriggerName));
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("TriggerName", TriggerName);
		}
	}
}