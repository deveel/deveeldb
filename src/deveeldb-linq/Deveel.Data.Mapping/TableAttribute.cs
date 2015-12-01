using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
	public sealed class TableAttribute : Attribute {
		public TableAttribute(string name) {
			this.TableName = name;
		}

		public TableAttribute()
			: this(null) {
		}

		public string TableName { get; set; }
	}
}