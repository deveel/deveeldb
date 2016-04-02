using System;

namespace Deveel.Data.Store.Journaled {
	class JournalEntry {
		public JournalEntry(JournalFile file, string resourceName, long position, long pageNumber) {
			File = file;
			ResourceName = resourceName;
			Position = position;
			PageNumber = pageNumber;
		}

		public JournalFile File { get; private set; }

		public string ResourceName { get; private set; }

		public long Position { get; private set; }

		public long PageNumber { get; private set; }

		public JournalEntry Next { get; set; }
	}
}
