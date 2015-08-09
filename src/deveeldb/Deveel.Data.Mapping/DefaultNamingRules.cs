using System;

namespace Deveel.Data.Mapping {
	[Flags]
	public enum DefaultNamingRules {
		CamelCase = 1,
		LowerCase = 2,
		UpperCase = 4,
		UnderscoreSeparator = 8,
	}
}
