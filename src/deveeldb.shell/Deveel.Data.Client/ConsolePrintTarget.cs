using System;
using System.IO;

namespace Deveel.Data.Client {
	public class ConsolePrintTarget : IPrintTarget {
		public int Width {
			get { return Console.WindowWidth; }
		}

		public int Height {
			get { return Console.WindowHeight; }
		}

		public TextWriter Output {
			get { return Console.Out; }
		}
	}
}
