using System;
using System.Collections.Generic;

namespace Deveel.Data.Security {
	public interface IResourceAccess {
		IEnumerable<ResourceAccessRequest> AccessRequests { get; }
	}
}
