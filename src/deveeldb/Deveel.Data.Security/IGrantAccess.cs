using System;
using System.Collections.Generic;

namespace Deveel.Data.Security {
	public interface IGrantAccess {
		IEnumerable<ResourceGrantRequest> GrantRequests { get; }
	}
}
