using System;

using SWFControl = System.Windows.Forms.Control;

namespace Deveel.Data {
	public interface IQueryEditor : IEditor, ITask, IBrowsableDocument, IQueryBatchHandler, ITextSearchProvider {
		SWFControl Control { get; }


		void SetStatus(string text);
	}
}