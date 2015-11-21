using System;

namespace Deveel.Data {
	public interface IQuery : IDisposable {
		IQueryContext QueryContext { get; }

		IUserSession Session { get;  }
	}
}
