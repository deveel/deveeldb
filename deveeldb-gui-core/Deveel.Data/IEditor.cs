using System;

namespace Deveel.Data {
	public interface IEditor {
		string FileName { get; set; }

		string FileFilter { get; }

		bool HasChanges { get; }

		string SelectedText { get; }

		string Text { get; }

		bool SupportsHistory { get; }


		void SetSyntax(string name);

		void LoadFile();

		void SaveFile();

		void Insert(string text);

		void ClearSelectedText();

		void HighlightText(int startIndex, int length);

		void Undo();

		void Redo();
	}
}