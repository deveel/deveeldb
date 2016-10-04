using System;

namespace Deveel.Data.Security {
	public interface ISecurityPostExecuteAction : ISecurityAction {
		void OnActionExecuted(ISecurityContext context);
	}
}
