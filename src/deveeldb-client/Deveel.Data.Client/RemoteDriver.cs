using System;
using System.IO;

namespace Deveel.Data.Client {
	internal class RemoteDriver : Driver {
		internal RemoteDriver(Stream stream, int queryTimeout)
			: base(queryTimeout) {
			this.stream = stream;
			input = new BinaryReader(new BufferedStream(stream, 32768));
			output = new BinaryWriter(new BufferedStream(stream, 32768));
		}

		private readonly BinaryReader input;
		private readonly BinaryWriter output;
		private Stream stream;

		protected override void WriteCommand(byte[] command, int offset, int size) {
			if (IsClosed)
				throw new InvalidOperationException();

			output.Write(size);
			output.Write(command, offset, size);
			output.Flush();
		}

		protected override byte[] ReadNextCommand(int timeout) {
			if (IsClosed)
				throw new InvalidOperationException();

			int size = input.ReadInt32();
			byte[] buffer = new byte[size];
			input.Read(buffer, 0, size);
			return buffer;
		}

		protected override void Dispose() {
			try {
				output.Flush();
			} finally {
				stream.Close();
				stream = null;
			}
		}
	}
}