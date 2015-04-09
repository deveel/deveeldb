using System;

namespace Deveel.Data.Security {
	[Flags]
	public enum Privileges {
		None = 0,
		Alter = 0x0100,
		Compact = 0x040,
		Create = 0x080,
		Delete = 0x02,
		Drop = 0x0200,
		Insert = 0x08,
		List = 0x0400,
		References = 0x010,
		Select = 0x01,
		Update = 0x04,
		Usage = 0x020,

		All = Alter | Compact | Create | Delete |
		      Drop | Insert | List | References |
		      Select | Update | Usage
	}
}