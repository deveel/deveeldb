using System;

namespace Deveel.Data.Sql.Cursors {
	public interface ICursorScope : IDisposable {
		CursorManager CursorManager { get; }
	}
}
