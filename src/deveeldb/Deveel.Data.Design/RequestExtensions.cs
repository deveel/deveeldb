using System;

using Deveel.Data.Linq;

namespace Deveel.Data.Design {
	public static class RequestExtensions {
		public static DbCompiledModel CompiledModel(this IRequest request) {
			return request.Query.Session.GetObjectModel();
		}
	}
}
