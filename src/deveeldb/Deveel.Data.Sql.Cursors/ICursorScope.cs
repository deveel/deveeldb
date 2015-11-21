using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Cursors {
	public interface ICursorScope : IContext {
		bool IgnoreCase { get; }

		CursorManager CursorManager { get; }
	}
}
