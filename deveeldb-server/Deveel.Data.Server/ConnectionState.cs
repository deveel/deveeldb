using System;

namespace Deveel.Data.Server {
	public enum ConnectionState {
		Closed = 0,
		NotAuthenticated = 4,
		Processing = 100
	}
}