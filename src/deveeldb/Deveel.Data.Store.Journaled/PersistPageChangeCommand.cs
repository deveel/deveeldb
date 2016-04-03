using System;
using System.IO;

namespace Deveel.Data.Store.Journaled {
	class PersistPageChangeCommand : PersistCommand {
		public PersistPageChangeCommand(long pageNumber, int offset, int count, Stream source)
			: base(PersistCommandType.PageChange) {
			PageNumber = pageNumber;
			Offset = offset;
			Count = count;
			Source = source;
		}

		public long PageNumber { get; private set; }

		public int Offset { get; private set; }

		public int Count { get; private set; }

		public Stream Source { get; private set; }
	}
}
