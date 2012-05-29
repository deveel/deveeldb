using System;
using System.IO;

namespace Deveel.Data.Control {
	public interface IConfigFormatter {
		void LoadFrom(DbConfig config, Stream inputStream);

		void SaveTo(DbConfig config, Stream outputStream);
	}
}