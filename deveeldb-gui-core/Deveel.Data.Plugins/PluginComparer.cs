using System;
using System.Collections;

namespace Deveel.Data.Plugins {
	public sealed class PluginComparer : IComparer {
		private PluginComparer() {
		}


		public static PluginComparer Instance = new PluginComparer();

		public int Compare(object x, object y) {
			if (x == null && y == null)
				return 0;
			if (x == null)
				return 1;
			if (y == null)
				return -1;

			IPlugin a = (IPlugin) x;
			IPlugin b = (IPlugin) y;

			return a.Order - b.Order;
		}
	}
}