using System;

namespace Deveel.Data.Security {
	public sealed class SecurityAssertionAction : ISecurityBeforeExecuteAction {
		void ISecurityBeforeExecuteAction.OnExecuteAction(ISecurityContext context) {
			var result = context.Assert();
			if (result.IsDenied)
				throw result.SecurityError;
		}
	}
}
