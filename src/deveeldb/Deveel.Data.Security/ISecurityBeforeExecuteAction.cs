using System;

namespace Deveel.Data.Security {
	public interface ISecurityBeforeExecuteAction : ISecurityAction {
		void OnExecuteAction(ISecurityContext context);
	}
}
