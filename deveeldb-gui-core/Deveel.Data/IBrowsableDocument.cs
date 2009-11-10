using System;

namespace Deveel.Data {
	public interface IBrowsableDocument : ICursorOffsetHandler {
		int Line { get; }

		int Column { get; }

		int TotalLines { get; }


		bool SetCursorOffset(int value);

		bool SetCursorPosition(int line, int column);
	}
}