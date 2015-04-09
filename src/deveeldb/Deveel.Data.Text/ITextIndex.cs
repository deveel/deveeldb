using System;

namespace Deveel.Data.Text {
	public interface ITextIndex {
		void Insert(int rowIndex, string fieldName, string value);

		void Remove(int rowIndex, string fieldName);
	}
}
