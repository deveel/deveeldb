using System;

namespace Deveel.Data.Server {
	public enum ClientConnectionState {
		Closed = 0,
		NotAuthenticated = 4,
		Processing = 100
	}
}