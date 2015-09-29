using System;

namespace Deveel.Data.Store.Journaled {
	enum JournalFileCommand : byte {
		TagResource = 2,
		DeleteResource = 6,
		ResourceSizeChange = 3,
		ModifyPage = 1,
		Checkpoint = 100
	}
}
