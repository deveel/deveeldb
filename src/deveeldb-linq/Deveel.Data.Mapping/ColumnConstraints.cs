using System;

namespace Deveel.Data.Mapping {
	[Flags]
	public enum ColumnConstraints {
		NotNull = 1,
		PrimaryKey = 2,
		Unique = 4
	}
}
