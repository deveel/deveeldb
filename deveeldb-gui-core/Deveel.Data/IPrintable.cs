using System;
using System.Drawing.Printing;

namespace Deveel.Data {
	public interface IPrintable {
		PrintDocument PrintDocument { get; }
	}
}