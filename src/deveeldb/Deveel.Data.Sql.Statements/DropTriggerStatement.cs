using System;
using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropTriggerStatement : SqlStatement {
		public DropTriggerStatement(ObjectName triggerName) {
			if (triggerName == null)
				throw new ArgumentNullException("triggerName");

			TriggerName = triggerName;
		}

		private DropTriggerStatement(ObjectData data)
			: base(data) {
			TriggerName = data.GetValue<ObjectName>("TriggerName");
		}

		public ObjectName TriggerName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			base.ExecuteStatement(context);
		}

		protected override void GetData(SerializeData data) {
			data.SetValue("TriggerName", TriggerName);
		}
	}
}