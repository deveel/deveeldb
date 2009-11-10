using System;

namespace Deveel.Data {
	public interface ITextSearchService {
		TextSearch SearchNext(TextSearch search);
	}
}