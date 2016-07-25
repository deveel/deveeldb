using System;
using System.IO;

namespace Deveel.Data.Client {
	public interface IPrintable {
		void WriteTo(IPrintTarget target);
	}
}
