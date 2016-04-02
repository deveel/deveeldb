using System;
using System.Collections.Generic;

namespace Deveel.Data.Store.Journaled {
	class JournalSummary {
		public JournalSummary(JournalFile journalFile) {
			JournalFile = journalFile;
			Resources = new List<string>();
		}

		public JournalFile JournalFile { get; private set; }

		public bool CanBeRecovered { get; set; }

		public long LastCheckPoint { get; set; }

		public ICollection<string> Resources { get; private set; } 
	}
}
