using System;
using System.Collections.Generic;

namespace Deveel.Data.Security {
	public interface ISecurityContext {
		IRequest Request { get; }

		User User { get; }

		object Target { get; }

		IEnumerable<ISecurityAssert> Assertions { get; }
	}
}
