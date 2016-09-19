using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false)]
	public sealed class UniqueAttribute : ColumnConstraintAttribute {
		public UniqueAttribute()
			: base(ColumnConstraintType.Unique) {
		}
	}
}
