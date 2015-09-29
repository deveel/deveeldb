using System;

namespace Deveel.Data.Store.Journaled {
	class JournalEntry {
		public JournalEntry(JournalRegistry registry, string resourceName, long offset, long pageNumber) {
			Registry = registry;
			ResourceName = resourceName;
			Offset = offset;
			PageNumber = pageNumber;
		}

		public string ResourceName { get; private set; }

		public long Offset { get; private set; }

		public long PageNumber { get; private set; }

		public JournalRegistry Registry { get; private set; }

		public JournalEntry Next { get; set; }
	}
}
