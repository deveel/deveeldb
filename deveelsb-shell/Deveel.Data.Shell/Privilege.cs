using System;

namespace Deveel.Data.Shell {
	public sealed class Privilege {
		private readonly string grantor;
		private readonly string grantee;
		private readonly bool grantable;

		internal Privilege(string grantor, string grantee, bool grantable) {
			this.grantor = grantor;
			this.grantable = grantable;
			this.grantee = grantee;
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
	}
}