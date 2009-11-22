using System;
using System.Collections.Generic;

namespace Deveel.Data.Search {
	public static class SearchTable {
		private static readonly Dictionary<int, TextSearch> searches = new Dictionary<int, TextSearch>();

		public static Dictionary<int , TextSearch> Searches {
			get { return searches; }
		}
	}
}