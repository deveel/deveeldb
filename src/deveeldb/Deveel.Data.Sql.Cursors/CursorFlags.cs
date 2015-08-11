using System;

namespace Deveel.Data.Sql.Cursors {
	[Flags]
	public enum CursorFlags {
		Insensitive = 1,
		Scroll = 2
	}
}
