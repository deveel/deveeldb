using System;

namespace Deveel.Data {
	public interface IDatabaseCreateCallback {
		void OnDatabaseCreate(IQuery systemQuery);
	}
}
