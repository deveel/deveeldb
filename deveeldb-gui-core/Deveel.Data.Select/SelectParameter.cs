using System;

namespace Deveel.Data.Select {
	public sealed class SelectParameter {
		internal SelectParameter(int id, string name) {
			this.id = id;
			this.name = name;
		}

		private readonly int id;
		private readonly string name;

		public string Name {
			get { return name; }
		}

		public int Id {
			get { return id; }
		}
	}
}