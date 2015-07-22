using System;

namespace Deveel.Data.Index {
	public sealed class LuceneIndexFactory : IIndexFactory {
		ColumnIndex IIndexFactory.CreateIndex(ColumnIndexContext context) {
			return CreateIndex(context);
		}

		public LuceneIndex CreateIndex(ColumnIndexContext context) {
			throw new NotImplementedException();
		}
	}
}
