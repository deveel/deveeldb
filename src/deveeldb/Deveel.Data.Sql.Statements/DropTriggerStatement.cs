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

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropTriggerStatement : SqlStatement {
		public DropTriggerStatement(ObjectName triggerName) {
			if (triggerName == null)
				throw new ArgumentNullException("triggerName");

			TriggerName = triggerName;
		}

		private DropTriggerStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			TriggerName = (ObjectName) info.GetValue("TriggerName", typeof(ObjectName));
		}

		public ObjectName TriggerName { get; private set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var triggerName = context.Access().ResolveObjectName(DbObjectType.Trigger, TriggerName);
			return new DropTriggerStatement(triggerName);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.DirectAccess.TriggerExists(TriggerName))
				throw new ObjectNotFoundException(TriggerName);
			if (!context.User.CanDrop(DbObjectType.Trigger, TriggerName))
				throw new SecurityException(String.Format("User '{0}' has not enough rights to drop trigger '{1}'.", context.User.Name, TriggerName));

			context.DirectAccess.DropTrigger(TriggerName);
			context.DirectAccess.RevokeAllGrantsOn(DbObjectType.Trigger, TriggerName);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("TriggerName", TriggerName);
		}
	}
}