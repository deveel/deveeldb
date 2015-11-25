using System;

using Deveel.Data.Security;

namespace Deveel.Data {
	public static class RequestExtensions {
		public static User User(this IRequest request) {
			return request.Query.User();
		}
	}
}
