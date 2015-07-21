using System;

namespace Deveel.Data.Sql.Triggers {
	public interface ITriggerListener {
		void OnTriggerEvent(TriggerEvent trigger);
	}
}
