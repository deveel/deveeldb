using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Variables {
	public static class ContextExtensions {
		public static Variable FindVariable(this IContext context, string variableName) {
			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is IVariableScope) {
					// TODO: get a Variable Manager for the context
				}

				currentContext = context.Parent;
			}


			// not found in the hierarchy
			return null;
		}
	}
}
