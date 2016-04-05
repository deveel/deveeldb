using System;

namespace Deveel.Data {
	public interface ISystemDisposeCallback {
		void OnDispose(ISystem system);
	}
}
