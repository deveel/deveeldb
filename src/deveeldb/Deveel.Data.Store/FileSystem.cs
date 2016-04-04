using System;

namespace Deveel.Data.Store {
	public static class FileSystem {
		static FileSystem() {
#if PCL
			Local = new PortableFileSystem();
#else
			Local = new LocalFileSystem();
#endif
		}

		public static IFileSystem Local { get; private set; }
	}
}
