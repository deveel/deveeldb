using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Cursors {
	public interface ICursor : IDbObject, IEnumerable<Row>, IDisposable {
		CursorStatus Status { get; }

		IRequest Context { get; }

		Row Fetch(FetchDirection direction, int offset);
	}
}
