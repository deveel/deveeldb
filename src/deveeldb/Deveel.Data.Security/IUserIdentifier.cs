using System;

namespace Deveel.Data.Security {
	public interface IUserIdentifier {
		string Name { get; }

		bool VerifyIdentification(string input, UserIdentification identification);

		UserIdentification CreateIdentification(string input);
	}
}
