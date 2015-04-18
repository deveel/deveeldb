using System;

namespace Deveel.Data.Security {
	public static class SecurityErrorCodes {
		public const int Unknown = 0x0010200;
		public const int InvalidAccess = 0x0024401;
		public const int MissingPrivileges = 0x0029399;
		public const int UserLocked = 0x0085994;
	}
}
