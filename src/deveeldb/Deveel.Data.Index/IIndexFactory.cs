using System;

namespace Deveel.Data.Index {
	public interface IIndexFactory {
		ColumnIndex CreateIndex(ColumnIndexContext context);
	}
}
