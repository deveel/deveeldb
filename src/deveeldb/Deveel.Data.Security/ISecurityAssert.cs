using System;

namespace Deveel.Data.Security {
	public interface ISecurityAssert {
		AssertResult Assert(ISecurityContext context);
	}
}
