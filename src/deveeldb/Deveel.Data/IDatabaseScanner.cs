using System;
using System.Collections.Generic;

namespace Deveel.Data {
	public interface IDatabaseScanner {
		IEnumerable<IDatabase> ScanDatabases();
	}
}
