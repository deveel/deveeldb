using System;

namespace Deveel.Data.Sql.Triggers {
	public interface ITableStateHandler {
		/// <summary>
		/// Gets an object that olds the state before and after a table event.
		/// </summary>
		OldNewTableState TableState { get; }

		void SetTableState(OldNewTableState tableState);
	}
}
