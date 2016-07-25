using System;
using System.IO;

namespace Deveel.Data.Client {
	public class TextOutputTarget : IOutputTarget {
		private bool disposed;

		public TextOutputTarget(TextWriter output) {
			Output = output;
		}

		public TextWriter Output { get; private set; }

		protected bool IsDisposed {
			get { return disposed; }
		}

		~TextOutputTarget() {
			Dispose(false);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void AssertNotDisposed() {
			if (disposed)
				throw new ObjectDisposedException(GetType().FullName);
		}

		protected virtual void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					if (Output != null)
						Output.Dispose();
				}

				Output = null;
				disposed = true;
			}
		}

		public virtual bool IsInteractive {
			get { return Environment.UserInteractive; }
		}

		public void Write(char[] buffer, int offset, int length) {
			AssertNotDisposed();
			Output.Write(buffer, offset, length);
		}

		public void Flush() {
			AssertNotDisposed();
			Output.Flush();
		}

		public virtual void SetAttribute(OutputAttributes attributes) {
			
		}

		public void Close() {
			AssertNotDisposed();
			Output.Close();
		}
	}
}
