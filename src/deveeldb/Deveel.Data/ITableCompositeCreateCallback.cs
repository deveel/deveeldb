using System;

namespace Deveel.Data {
	public interface ITableCompositeCreateCallback {
		void OnTableCompositeCreate(IQuery systemQuery);
	}
}
