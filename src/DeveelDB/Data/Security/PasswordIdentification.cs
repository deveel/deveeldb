using System;

namespace Deveel.Data.Security {
	public class PasswordIdentification : IUserIdentification {
		public PasswordIdentification(string password) {
			Password = password;
		}

		public string Password { get; }
	}
}