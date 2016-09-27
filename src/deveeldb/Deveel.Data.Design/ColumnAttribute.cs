using System;

namespace Deveel.Data.Design {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class ColumnAttribute : Attribute {
		public string Name { get; set; }

		public string Type { get; set; }

		public string TypeName { get; set; }

		public int Precision { get; set; }

		public int Scale { get; set; }

		public int Size { get; set; }
	}
}
