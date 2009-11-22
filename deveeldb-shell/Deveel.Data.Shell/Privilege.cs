using System;

namespace Deveel.Data.Shell {
	public sealed class Privilege {
		private readonly string grantor;
		private readonly string grantee;
		private readonly string priv;
		private readonly bool grantable;

		internal Privilege(string grantor, string grantee, string priv, bool grantable) {
			this.grantor = grantor;
			this.grantable = grantable;
			this.grantee = grantee;
			this.priv = priv;
		}

		public bool IsGrantable {
			get { return grantable; }
		}

		public string Grantee {
			get { return grantee; }
		}

		public string Grantor {
			get { return grantor; }
		}

		public string Value {
			get { return priv; }
		}
	}
}