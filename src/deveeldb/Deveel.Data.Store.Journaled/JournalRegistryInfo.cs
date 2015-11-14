using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Store.Journaled {
	class JournalRegistryInfo {
		private readonly List<string> resources;
		 
		public JournalRegistryInfo(JournalRegistry registry) {
			Registry = registry;
			resources = new List<string>();
		}

		public JournalRegistry Registry { get; private set; }

		public long LastCheckPoint { get; internal set; }

		public IEnumerable<string> Resources {
			get { return resources.AsEnumerable(); }
		}

		public bool Recoverable { get; internal set; }

		internal void AddResources(IEnumerable<string> names) {
			resources.AddRange(names);
		}
	}
}
