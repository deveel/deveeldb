using System;
using System.IO;

namespace Deveel.Data.Client {
	public interface IPrintTarget {
		int Width { get; }

		int Height { get; }

		TextWriter Output { get; }
	}
}
