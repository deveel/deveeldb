using System;

namespace Deveel.Data.Store.Journaled {
	class PersistSetSizeCommand : PersistCommand {
		public PersistSetSizeCommand(long newSize)
			: base(PersistCommandType.SetSize) {
			NewSize = newSize;
		}

		public long NewSize { get; private set; }
	}
}
