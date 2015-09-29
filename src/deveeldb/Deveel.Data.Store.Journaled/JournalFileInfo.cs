using System;
using System.Collections.Generic;

namespace Deveel.Data.Store.Journaled {
	class JournalFileInfo {
		public JournalFileInfo(JournalRegistry registry) {
			Registry = registry;
			Resources = new List<string>();
		}

		public JournalRegistry Registry { get; private set; }

		public bool CanBeRecovered { get; set; }

		public long CheckpointPosition { get; set; }

		public ICollection<string> Resources { get; private set; }
	}
}
