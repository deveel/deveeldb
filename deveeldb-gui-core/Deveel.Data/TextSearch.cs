using System;

namespace Deveel.Data {
	public class TextSearch {
		public TextSearch(ITextSearchProvider provider, string text) {
			this.text = text;
			this.provider = provider;
		}

		private readonly ITextSearchProvider provider;
		private readonly string text;
		private int position;
		private bool ignoreCase;

		public ITextSearchProvider Provider {
			get { return provider; }
		}

		public string Text {
			get { return text; }
		}

		public int Position {
			get { return position; }
			set { position = value; }
		}

		public bool IgnoreCase {
			get { return ignoreCase; }
			set { ignoreCase = value; }
		}
	}
}