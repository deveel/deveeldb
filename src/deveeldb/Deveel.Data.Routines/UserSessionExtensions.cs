using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;

namespace Deveel.Data.Routines {
	public static class UserSessionExtensions {
		public static IRoutine ResolveRoutine(this IUserSession session, Invoke invoke) {
			return session.GetObject(DbObjectType.Routine, invoke.RoutineName) as IRoutine;
		}
	}
}
