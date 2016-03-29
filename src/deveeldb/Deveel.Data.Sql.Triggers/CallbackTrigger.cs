using System;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Sql.Triggers {
	public sealed class CallbackTrigger : Trigger {
		public CallbackTrigger(CallbackTriggerInfo triggerInfo)
			: base(triggerInfo) {
		}

		public override void Fire(TableEvent tableEvent, IRequest context) {
			var e = new TriggerEvent(TriggerInfo.TriggerName, tableEvent.Table.FullName, tableEvent.EventType,
				tableEvent.OldRowId, tableEvent.NewRow);

			context.Context.RegisterEvent(e);
		}
	}
}
