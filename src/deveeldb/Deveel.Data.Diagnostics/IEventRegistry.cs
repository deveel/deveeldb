using System;

namespace Deveel.Data.Diagnostics {
	public interface IEventRegistry {
		void RegisterEvent(IEvent dbEvent);
	}
}
