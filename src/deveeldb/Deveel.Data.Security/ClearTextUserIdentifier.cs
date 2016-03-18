using System;

namespace Deveel.Data.Security {
	public sealed class ClearTextUserIdentifier : IUserIdentifier {
		public string Name {
			get { return KnownUserIdentifications.ClearText; }
		}

		public bool VerifyIdentification(string input, UserIdentification identification) {
			return String.Equals(input, identification.Token);
		}

		public UserIdentification CreateIdentification(string input) {
			if (String.IsNullOrEmpty(input))
				throw new ArgumentNullException("input");

			return new UserIdentification(Name, input);
		}
	}
}
