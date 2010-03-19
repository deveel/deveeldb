using System;

namespace Deveel.Data {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public sealed class GenerateDatabaseAttribute: Attribute {
		private readonly bool generate;

		public GenerateDatabaseAttribute(bool generate) {
			this.generate = generate;
		}

		public GenerateDatabaseAttribute()
			: this(true) {
		}

		public bool Generate {
			get { return generate; }
		}
	}
}