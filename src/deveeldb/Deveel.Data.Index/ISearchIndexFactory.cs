using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Index {
	public interface ISearchIndexFactory {
		ISearchIndex CreateIndex(IndexInfo indexInfo);
	}
}
