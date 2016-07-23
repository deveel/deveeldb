using System;

namespace Deveel.Data {
	static class RequestExtensions {
		internal static SystemAccess Access(this IRequest request) {
			if (request == null)
				throw new ArgumentNullException("request");

			if (!(request is IProvidesDirectAccess))
				return new RequestAccess(request);

			return ((IProvidesDirectAccess)request).DirectAccess;
		}
	}
}
