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
