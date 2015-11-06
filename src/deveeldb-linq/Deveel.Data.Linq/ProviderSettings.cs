using System;
using System.Collections.Generic;

using Deveel.Data.Mapping;

namespace Deveel.Data.Linq {
	public sealed class ProviderSettings {
		public ProviderSettings(string userName, string password, MappingModel model) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.IsNullOrEmpty(password))
				throw new ArgumentNullException("password");
			if (model == null)
				throw new ArgumentNullException("model");

			UserName = userName;
			Password = password;
			MappingModel = model;

			Metadata = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
		}

		public string UserName { get; private set; }

		public string Password { get; private set; }

		public IDictionary<string, object> Metadata { get; private set; }

		public MappingModel MappingModel { get; private set; }
	}
}
