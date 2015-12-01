using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
	public sealed class FunctionAttribute : Attribute {
		public FunctionAttribute(string name) {
			this.name = name;
		}

		public FunctionAttribute()
			: this(null) {
		}

		private string name;

		public string FunctionName {
			get { return name; }
			set { name = value; }
		}
	}
}