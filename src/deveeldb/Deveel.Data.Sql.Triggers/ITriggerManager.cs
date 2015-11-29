using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Triggers {
	public interface ITriggerManager : IDisposable {
		void RegisterTrigger(TriggerInfo triggerInfo);

		void FireTriggers(IQuery context, TableEventContext tableEvent);
	}
}
