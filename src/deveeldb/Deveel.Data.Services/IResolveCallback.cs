using System;

namespace Deveel.Data.Services {
	public interface IResolveCallback {
		void OnResolved(IResolveScope scope);
	}
}
