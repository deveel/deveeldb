using System;
using System.Collections.Generic;
using System.Text;

using Deveel.Configuration;

namespace sqlstate {
	class Program {
		private static Options GetOptions() {
			Options options = new Options();
			options.AddOption("command", "c", true, "The kind of command to apply (either 'add' or 'remove')");
			options.AddOption("code", "x", true, "The code of the state.");
			return options;
		}

		static void Main(string[] args) {
		}
	}
}
