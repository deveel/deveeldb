using System;

namespace Deveel.Data {
	public interface ITableCompositeSetupCallback {
		void OnTableCompositeSetup(IQuery systemQuery);
	}
}
