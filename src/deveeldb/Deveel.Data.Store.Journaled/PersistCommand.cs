using System;

namespace Deveel.Data.Store.Journaled {
	class PersistCommand {
		public PersistCommand(PersistCommandType commandType) {
			CommandType = commandType;
		}

		public PersistCommandType CommandType { get; private set; }
	}
}
