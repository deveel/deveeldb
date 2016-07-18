using System;

namespace Deveel.Data {
	static class SessionExtensions {
		internal static SystemAccess Access(this ISession session) {
			if (!(session is IProvidesDirectAccess))
				throw new InvalidOperationException("The session does not provide direct access to the system.");

			return ((IProvidesDirectAccess)session).DirectAccess;
		}
	}
}
