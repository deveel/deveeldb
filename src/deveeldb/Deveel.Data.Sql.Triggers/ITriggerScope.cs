using System;

namespace Deveel.Data.Sql.Triggers {
	public interface ITriggerScope {
		ITriggerManager TriggerManager { get; }

		void OnTriggerEvent(TriggerEvent e);
	}
}
