using System;

namespace Deveel.Data.Client {
	public interface IOutputTarget : IDisposable {
		bool IsInteractive { get; }

		void Write(char[] buffer, int offset, int length);

		void Flush();

		void SetAttribute(OutputAttributes attributes);

		void Close();
	}
}
