using System;

namespace Deveel.Data {
	public interface ITextSearchProvider : ICursorOffsetHandler {
		ITextSearchService TextSearchService { get; }

		bool SupportsReplace { get; }


		int Search(string value, int offset, bool ignoreCase);

		bool Replace(string newValue, int offset, int length);
	}
}