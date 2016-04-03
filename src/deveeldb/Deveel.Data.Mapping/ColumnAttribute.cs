using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class ColumnAttribute : Attribute {
		public string Name { get; set; }

		public string TypeName { get; set; }

		public object Default { get; set; }

		public bool DefaultIsExpression { get; set; }

		public bool Null { get; set; }
	}
}
