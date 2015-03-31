using System;

namespace Deveel.Data.DbSystem {
	public enum TableRecordState : byte {
		Added = 0x010,
		Removed = 0x020
	}
}
