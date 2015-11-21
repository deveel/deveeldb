using System;

namespace Deveel.Data.Sql.Cursors {
	public interface ICursorScope : IDisposable {
		bool IgnoreCase { get; }

		CursorManager CursorManager { get; }
	}
}
