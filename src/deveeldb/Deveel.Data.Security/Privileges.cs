using System;

namespace Deveel.Data.Security {
	[Flags]
	public enum Privileges {
		None = 0,
		Create = 1,
		Alter = 2,
		Compact = 1024,
		Delete = 64,
		Drop = 4,
		Insert = 128,
		List = 8,
		References = 256,
		Select = 16,
		Update = 32,
		Usage = 512,
		Execute = 2048,

		All = Alter | Compact | Create | Delete |
		      Drop | Insert | List | References |
		      Select | Update | Usage,

		TableAll = Select | Update | Delete | Insert | References | Usage | Compact,
		TableRead = Select | Usage,

		SchemaAll = Create | Alter | Drop | List,
		SchemaRead = List,
	}
}