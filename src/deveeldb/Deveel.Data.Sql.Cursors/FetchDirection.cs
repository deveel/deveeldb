using System;

namespace Deveel.Data.Sql.Cursors {
	public enum FetchDirection {
		Next = 1,
		Prior = 2,
		First = 3,
		Last = 4,
		Absolute = 5,
		Relative = 6
	}
}
