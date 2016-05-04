using System;

namespace Deveel.Data.Security {
	public static class PrivilegeSets {
		public static readonly Privileges TableAll = Privileges.Select |
		                                             Privileges.Update |
		                                             Privileges.Delete |
		                                             Privileges.Insert |
		                                             Privileges.References |
		                                             Privileges.Usage | Privileges.Compact;

		public static readonly Privileges TableRead = Privileges.Select | Privileges.Usage;

		public static readonly Privileges SchemaAll = Privileges.Create |
		                                              Privileges.Alter |
		                                              Privileges.Drop |
		                                              Privileges.List;

		public static readonly Privileges SchemaRead = Privileges.List;
	}
}
