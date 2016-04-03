using System;

namespace Deveel.Data.Store.Journaled {
	enum PersistCommandType {
		Open,
		Close,
		Delete,
		SetSize,
		PageChange,
		Synch,
		PostRecover
	}
}
