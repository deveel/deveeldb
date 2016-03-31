using System;

namespace Deveel.Data.Sql {
	public interface ISystemCreateCallback {
		void Activate(SystemCreatePhase phase);
	}
}
