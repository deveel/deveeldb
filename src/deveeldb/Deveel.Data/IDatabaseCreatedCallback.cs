using System;

namespace Deveel.Data {
	public interface IDatabaseCreatedCallback {
		void OnDatabaseCreated(IQuery systemQuery);
	}
}
