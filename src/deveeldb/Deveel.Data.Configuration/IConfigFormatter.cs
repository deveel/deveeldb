using System;
using System.IO;

namespace Deveel.Data.Configuration {
	public interface IConfigFormatter {
		void LoadInto(IDbConfig config, Stream inputStream);

		void SaveFrom(IDbConfig config, Stream outputStream);
	}
}