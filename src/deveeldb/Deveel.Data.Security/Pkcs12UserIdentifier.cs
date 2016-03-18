using System;

namespace Deveel.Data.Security {
	public sealed class Pkcs12UserIdentifier : IUserIdentifier {
		public string Name {
			get { return KnownUserIdentifications.Pkcs12; }
		}

		public bool VerifyIdentification(string input, UserIdentification identification) {
			throw new NotImplementedException();
		}

		public UserIdentification CreateIdentification(string input) {
			throw new NotImplementedException();
		}
	}
}
