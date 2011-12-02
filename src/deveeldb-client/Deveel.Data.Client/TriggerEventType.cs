using System;

namespace Deveel.Data.Client {
	[Flags]
	public enum TriggerEventType {
		Insert = 0x001,
		Delete = 0x002,
		Update = 0x004,

		Before = 0x010,
		After = 0x020,

		BeforeInsert = Before | Insert,

		BeforeDelete = Before | Delete,

		BefroeUpdate = Before | Update,

		AfterInsert = After | Insert,

		AfterUpdate = After | Update,

		AfterDelete = After | Delete
	}
}