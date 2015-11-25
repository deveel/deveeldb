using System;

namespace Deveel.Data {
	public interface IQuery : IRequest {
		IQueryContext QueryContext { get; }

		IUserSession Session { get;  }
	}
}
