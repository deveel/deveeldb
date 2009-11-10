using System;

namespace Deveel.Data {
	public class TextSearchService : ITextSearchService {
		public TextSearch SearchNext(TextSearch search) {
			if (search == null)
				throw new ArgumentNullException("search");

			string text = search.Text;
			int offset = 0;
			int textLength = 0;

			if (text != null && text.Length > 0) {
				offset = search.Provider.Search(text, search.Position, search.IgnoreCase);
				textLength = text.Length;
			}

			if (offset != -1)
				search.Position = offset + textLength;

			return search;
		}
	}
}