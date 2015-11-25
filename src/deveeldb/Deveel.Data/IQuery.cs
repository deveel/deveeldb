using System;

namespace Deveel.Data {
	public interface IQuery : IRequest {
		new IQueryContext Context { get; }

		ISession Session { get;  }
	}
}
