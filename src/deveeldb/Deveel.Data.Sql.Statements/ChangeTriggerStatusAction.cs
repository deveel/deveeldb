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

using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class ChangeTriggerStatusAction : IAlterTriggerAction, ISerializable {
		public ChangeTriggerStatusAction(TriggerStatus status) {
			if (status == TriggerStatus.Unknown)
				throw new ArgumentException("Invalid status for the action");

			Status = status;
		}

		private ChangeTriggerStatusAction(SerializationInfo info, StreamingContext context) {
			Status = (TriggerStatus) info.GetByte("Status");
		}

		public TriggerStatus Status { get; private set; }

		AlterTriggerActionType IAlterTriggerAction.ActionType {
			get { return AlterTriggerActionType.ChangeStatus; }
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Status", (byte)Status);
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			if (Status == TriggerStatus.Enabled) {
				builder.Append("ENABLE");
			} else {
				builder.Append("DISABLE");
			}
		}
	}
}
