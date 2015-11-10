using System;
using System.Diagnostics;

namespace Deveel.Data.Security {
	[DebuggerDisplay("{Name}")]
	public sealed class UserInfo {
		public UserInfo(string name, UserIdentification identification) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");
			if (identification == null)
				throw new ArgumentNullException("identification");

			Name = name;
			Identification = identification;
		}

		public string Name { get; private set; }
		
		public UserIdentification Identification { get; private set; }
	}
}
