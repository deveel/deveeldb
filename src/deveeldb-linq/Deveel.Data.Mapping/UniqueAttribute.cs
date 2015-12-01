using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
	public sealed class UniqueAttribute : ColumnConstraintAttribute, INamedConstraint {
		public UniqueAttribute(string name)
			: base(ColumnConstraints.Unique) {
			this.ConstraintName = name;
		}

		public UniqueAttribute()
			: this(null) {
		}

		public string ConstraintName { get; set; }
	}
}