using System;

namespace Deveel.Data.Store.Journaled {
	class PersistOpenCommand : PersistCommand {
		public PersistOpenCommand(bool readOnly)
			: base(PersistCommandType.Open) {
			ReadOnly = readOnly;
		}

		public bool ReadOnly { get; private set; }
	}
}
