using System;

namespace Deveel.Data.Client {
	[Flags]
	public enum TriggerEventTypes {
		Insert = 1,
		Update = 2,
		Delete = 4,
		All = Insert | Update | Delete
	}
}