using System;

namespace Deveel.Data {
	public interface ISystemCreateCallback {
		void OnCreated(ISystem system);
	}
}
