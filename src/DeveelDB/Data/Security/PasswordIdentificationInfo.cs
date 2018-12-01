using System;

namespace Deveel.Data.Security {
	public class PasswordIdentificationInfo : IUserIdentificationInfo {
		public PasswordIdentificationInfo(string password) {
			Password = password;
		}

		public string Password { get; }

		public int? ExpirationDays { get; set; }
	}
}