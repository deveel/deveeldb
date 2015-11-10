using System;

namespace Deveel.Data.Security {
	public interface IAuthenticationCallback {
		bool Authenticate(User user);
	}
}
