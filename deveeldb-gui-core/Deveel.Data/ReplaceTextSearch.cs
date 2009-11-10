using System;

namespace Deveel.Data {
	public class ReplaceTextSearch : TextSearch {
		public ReplaceTextSearch(ITextSearchProvider provider, string text, string replace) 
			: base(provider, text) {
			this.replace = replace;
		}

		private readonly string replace;

		public string Replace {
			get { return replace; }
		}
	}
}