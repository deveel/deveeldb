using System;

namespace Deveel.Data.Sql.Triggers {
	public interface ITriggerScope {
		ITriggerManager TriggerManager { get; }
	}
}
