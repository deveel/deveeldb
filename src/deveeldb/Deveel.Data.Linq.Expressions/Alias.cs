using System;

namespace Deveel.Data.Linq.Expressions {
	public sealed class Alias {
		public string Name {
			get { return String.Format("A{0}", GetHashCode()); }
		}

		public override string ToString() {
			return Name;
		}
	}
}
