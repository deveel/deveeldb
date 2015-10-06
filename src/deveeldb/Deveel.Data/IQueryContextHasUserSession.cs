using System;

namespace Deveel.Data {
	interface IQueryContextHasUserSession : IQueryContext {
		IUserSession Session { get; }
	}
}
