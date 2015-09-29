using System;

namespace Deveel.Data.Store.Journaled {
	public interface IFileHandleFactory {
		IFileHandle CreateHandle(string fileName, bool readOnly);

		bool FileExists(string fileName);
	}
}
