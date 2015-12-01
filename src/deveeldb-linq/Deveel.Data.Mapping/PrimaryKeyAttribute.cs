using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class PrimaryKeyAttribute : ColumnConstraintAttribute, INamedConstraint {
		public PrimaryKeyAttribute(string name)
			: base(ColumnConstraints.PrimaryKey) {
			this.ConstraintName = name;
		}

		public PrimaryKeyAttribute()
			: this(null) {
		}

		public string ConstraintName { get; set; }
	}
}